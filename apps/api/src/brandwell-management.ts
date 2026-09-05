import { timingSafeEqual } from "node:crypto";
import {
  acquireBrandwellModelPolicyLease,
  BRANDWELL_OPENROUTER_PROVIDER,
  type BrandwellProvisioningCheckpoint,
  BrandwellProvisioningError,
  type BrandwellProvisioningInput,
  BrandwellSidekickError,
  type BrandwellSidekickProvisioningInput,
  type BrandwellWorkspaceDesiredStateInput,
  brandwellOutreachFollowupPrompt,
  brandwellPlatformModelDefault,
  parseBrandwellOutreachFollowup,
} from "@brandwell/aimee";
import {
  type JobPublisher,
  routineJobKey,
  routineWakeupJob,
  runContinueJob,
} from "@rakazo/adapter-kit";
import {
  hasMixedOneShotSchedule,
  isOneShotRoutineCrons,
  nextCronDateAcrossStrict,
} from "@rakazo/core";
import type { PrismaClient } from "@rakazo/db";
import type { Context, Hono } from "hono";
import type { BrandwellSupportActor } from "./brandwell-support.js";
import { BrandwellSupportComputerError } from "./brandwell-support.js";

export interface BrandwellManagementDeps {
  prisma: PrismaClient;
  token: string;
  jobs?: JobPublisher;
  provisionWorkspace?: (
    input: BrandwellProvisioningInput,
  ) => Promise<BrandwellProvisioningCheckpoint>;
  cancelWorkspace?: (
    workspaceId: string,
    reason: string | undefined,
  ) => Promise<{ retentionEndsAt: Date; executed: string[] }>;
  syncDesiredState?: (
    workspaceId: string,
    input: BrandwellWorkspaceDesiredStateInput,
  ) => Promise<{ mapping: { commercialRevision: bigint }; replayed: boolean }>;
  provisionSidekick?: (
    workspaceId: string,
    input: BrandwellSidekickProvisioningInput,
  ) => Promise<Record<string, unknown>>;
  setSidekickLifecycle?: (
    sidekickId: string,
    action: "pause" | "resume" | "cancel",
    input: {
      idempotencyKey: string;
      auditMetadata: Record<string, string>;
    },
  ) => Promise<Record<string, unknown>>;
  rolloutSkillBundle?: (workspaceId: string) => Promise<Record<string, unknown>>;
  reconcileModelUsage?: (workspaceId: string) => Promise<unknown>;
  updateOpenRouterLimit?: (keyHash: string, monthlyLimitMicros: bigint) => Promise<void>;
  validateOpenRouterModel?: (modelId: string) => Promise<ManagedModelCatalogEntry | null>;
  computerSupport?: {
    boot(input: BrandwellSupportRequest): Promise<unknown>;
    takeControl(input: BrandwellSupportRequest): Promise<unknown>;
    screen(input: BrandwellSupportRequest): Promise<unknown>;
    release(input: BrandwellSupportRequest): Promise<unknown>;
  };
}

type ManagedModelCatalogEntry = {
  id: string;
  name: string;
  inputModalities: string[];
  outputModalities: string[];
  supportedParameters: string[];
  reasoning: boolean;
  contextLength?: number;
  maxCompletionTokens?: number;
  pricing: {
    prompt?: string;
    completion?: string;
    inputCacheRead?: string;
    inputCacheWrite?: string;
  };
};

type BrandwellSupportRequest = {
  workspaceId: string;
  botId?: string | null;
  actor: BrandwellSupportActor;
  reason?: string;
};

type WorkspaceMapping = Awaited<ReturnType<typeof findWorkspaceMapping>>;
type ClientNotificationRecord = {
  id: string;
  type: string;
  title: string;
  severity: string;
  requiresAction: boolean;
  createdAt: Date;
};

export function constantTimeBearerMatches(header: string | undefined, expected: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  const supplied = header.slice("Bearer ".length).trim();
  const suppliedBytes = Buffer.from(supplied);
  const expectedBytes = Buffer.from(expected);
  return (
    suppliedBytes.length === expectedBytes.length && timingSafeEqual(suppliedBytes, expectedBytes)
  );
}

export function mountBrandwellManagementRoutes(app: Hono, deps: BrandwellManagementDeps): void {
  app.use("/internal/*", async (c, next) => {
    if (!constantTimeBearerMatches(c.req.header("authorization"), deps.token)) {
      return c.json({ error: "Unauthorized" }, 401);
    }
    await next();
  });

  app.get("/internal/model-policy", async (c) => {
    const defaultModel = await brandwellPlatformModelDefault(deps.prisma);
    const [inheritedWorkspaces, customWorkspaces, sidekickCredentials] = await Promise.all([
      deps.prisma.brandwellWorkspaceModelCredential.count({
        where: { inheritsPlatformModelDefault: true },
      }),
      deps.prisma.brandwellWorkspaceModelCredential.count({
        where: { inheritsPlatformModelDefault: false },
      }),
      deps.prisma.brandwellSidekickModelCredential.count(),
    ]);
    return c.json({
      provider: BRANDWELL_OPENROUTER_PROVIDER,
      defaultModel,
      inheritedWorkspaces,
      customWorkspaces,
      sidekickCredentials,
    });
  });

  app.post("/internal/model-policy", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = platformModelPolicyInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    if (!deps.validateOpenRouterModel) {
      return c.json({ error: "OpenRouter model validation is not configured" }, 503);
    }
    let metadata: ManagedModelCatalogEntry | null;
    try {
      metadata = await deps.validateOpenRouterModel(input.value.modelId);
    } catch {
      return c.json({ error: "OpenRouter model validation is temporarily unavailable" }, 503);
    }
    if (!metadata) {
      return c.json(
        { error: `${input.value.modelId} is unavailable or does not support AIMEE tools` },
        400,
      );
    }
    if (!metadata.inputModalities.includes("image")) {
      return c.json({ error: "The BrandWell default model must support image input" }, 400);
    }

    const previousDefault = await brandwellPlatformModelDefault(deps.prisma);
    const inherited = await deps.prisma.brandwellWorkspaceModelCredential.findMany({
      where: { inheritsPlatformModelDefault: true },
      select: { id: true, workspaceId: true, modelCatalog: true },
    });
    let sidekickCredentialsUpdated = 0;
    let workspacesUpdated = 0;
    try {
      await deps.prisma.$transaction(async (tx) => {
        await tx.deploymentSettings.upsert({
          where: { id: "default" },
          create: {
            id: "default",
            brandwellDefaultModelId: input.value.modelId,
          },
          update: { brandwellDefaultModelId: input.value.modelId },
        });
        for (const current of inherited) {
          const modelCatalog = {
            ...modelCatalogRecord(current.modelCatalog),
            [input.value.modelId]: metadata,
          };
          const updated = await tx.brandwellWorkspaceModelCredential.updateMany({
            where: { id: current.id, inheritsPlatformModelDefault: true },
            data: {
              preferredModel: input.value.modelId,
              modelCatalog,
            },
          });
          if (!updated.count) continue;
          workspacesUpdated += 1;
          const sidekicks = await tx.brandwellSidekickModelCredential.updateMany({
            where: { workspaceId: current.workspaceId },
            data: {
              preferredModel: input.value.modelId,
              modelCatalog,
            },
          });
          sidekickCredentialsUpdated += sidekicks.count;
          await tx.bot.updateMany({
            where: {
              workspaceId: current.workspaceId,
              managedByBrandWell: true,
              archivedAt: null,
            },
            data: {
              modelProvider: BRANDWELL_OPENROUTER_PROVIDER,
              modelId: input.value.modelId,
            },
          });
          await tx.brandwellAuditLog.create({
            data: {
              workspaceId: current.workspaceId,
              actorType: "brandwell_operator",
              action: "model.platform_default.update",
              resourceType: "model_policy",
              resourceId: current.id,
              metadata: {
                previousDefault,
                defaultModel: input.value.modelId,
                ...operatorAuditMetadata(operator.value),
              },
            },
          });
        }
      });
    } catch {
      return c.json({ error: "AIMEE could not save the BrandWell default model" }, 503);
    }
    return c.json({
      ok: true,
      provider: BRANDWELL_OPENROUTER_PROVIDER,
      defaultModel: input.value.modelId,
      workspacesUpdated,
      sidekickCredentialsUpdated,
      managedCredentialsUpdated: workspacesUpdated + sidekickCredentialsUpdated,
    });
  });

  app.get("/internal/workspaces", async (c) => {
    const limit = boundedLimit(c.req.query("limit"));
    const cursor = workspaceCursorInput(c.req.query("cursor"));
    if (!cursor.ok) return c.json({ error: cursor.error }, 400);
    const rows = await deps.prisma.brandwellAiWorkspace.findMany({
      take: limit + 1,
      ...(cursor.value
        ? {
            where: {
              OR: [
                { createdAt: { lt: cursor.value.createdAt } },
                { createdAt: cursor.value.createdAt, id: { gt: cursor.value.id } },
              ],
            },
          }
        : {}),
      orderBy: [{ createdAt: "desc" }, { id: "asc" }],
      include: { rakazoWorkspace: { select: { name: true, slug: true } } },
    });
    const hasMore = rows.length > limit;
    const page = rows.slice(0, limit);
    const last = page.at(-1);
    return c.json({
      workspaces: await fleetRows(deps.prisma, page),
      hasMore,
      nextCursor: hasMore && last ? encodeWorkspaceCursor(last.createdAt, last.id) : null,
    });
  });

  app.post("/internal/workspaces/provision", async (c) => {
    if (!deps.provisionWorkspace) {
      return c.json({ error: "AIMEE provisioning is not configured" }, 503);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = provisioningInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    try {
      const checkpoint = await deps.provisionWorkspace(input.value);
      const inviteStep = checkpoint.steps.find((step) => step.name === "client_admin_membership");
      const workspaceId = checkpoint.steps.find((step) => step.name === "workspace")?.resourceId;
      if (workspaceId) {
        await deps.prisma.brandwellAuditLog.create({
          data: {
            workspaceId,
            actorType: "brandwell_operator",
            action: "workspace.provision",
            resourceType: "brandwell_ai_workspace",
            resourceId: input.value.brandwellCustomerId,
            metadata: {
              runId: checkpoint.runId,
              ...operatorAuditMetadata(operator.value),
            },
          },
        });
      }
      return c.json({
        status: checkpoint.status,
        runId: checkpoint.runId,
        workspaceId: workspaceId ?? null,
        botId: checkpoint.steps.find((step) => step.name === "primary_aimee")?.resourceId ?? null,
        serviceIdentityId:
          checkpoint.steps.find((step) => step.name === "service_identity")?.resourceId ?? null,
        clientAccess: inviteStep
          ? {
              kind: inviteStep.metadata?.kind ?? "pending",
              resourceId: inviteStep.resourceId ?? null,
            }
          : null,
      });
    } catch (error) {
      if (error instanceof BrandwellProvisioningError) {
        const conflict = error.message.includes("already running");
        return c.json(
          {
            error: conflict ? "Provisioning is already running" : "AIMEE provisioning failed",
            status: error.checkpoint.status,
            runId: error.checkpoint.runId,
          },
          conflict ? 409 : 500,
        );
      }
      return c.json({ error: "AIMEE provisioning failed" }, 500);
    }
  });

  app.put("/internal/workspaces/:id/desired-state", async (c) => {
    if (!deps.syncDesiredState) {
      return c.json({ error: "AIMEE commercial synchronization is not configured" }, 503);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = desiredStateInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    try {
      const result = await deps.syncDesiredState(c.req.param("id"), input.value);
      const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
      if (mapping) {
        await deps.prisma.brandwellAuditLog.create({
          data: {
            workspaceId: mapping.rakazoWorkspaceId,
            actorType: "brandwell_operator",
            action: "workspace.desired_state.sync",
            resourceType: "brandwell_ai_workspace",
            resourceId: mapping.id,
            metadata: {
              revision: input.value.revision.toString(),
              commercialStatus: input.value.status,
              sidekickSeats: input.value.sidekickSeats,
              replayed: result.replayed,
              ...operatorAuditMetadata(operator.value),
            },
          },
        });
      }
      return c.json({
        ok: true,
        revision: result.mapping.commercialRevision.toString(),
        replayed: result.replayed,
      });
    } catch (error) {
      return brandwellSidekickError(c, error, "AIMEE commercial synchronization failed");
    }
  });

  app.get("/internal/workspaces/:id/sidekicks", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const sidekicks = await deps.prisma.brandwellSidekick.findMany({
      where: { aiWorkspaceId: mapping.id },
      include: { bot: true, computer: true },
      orderBy: [{ createdAt: "asc" }, { id: "asc" }],
    });
    return c.json({ sidekicks: sidekicks.map((sidekick) => sidekickDto(sidekick)) });
  });

  app.post("/internal/workspaces/:id/sidekicks", async (c) => {
    if (!deps.provisionSidekick) {
      return c.json({ error: "AIMEE Sidekick provisioning is not configured" }, 503);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = sidekickProvisioningInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    try {
      const result = await deps.provisionSidekick(c.req.param("id"), input.value);
      await deps.prisma.brandwellAuditLog.create({
        data: {
          workspaceId: mapping.rakazoWorkspaceId,
          actorType: "brandwell_operator",
          action: "sidekick.provision",
          resourceType: "brandwell_sidekick",
          resourceId: input.value.brandwellSidekickId,
          metadata: {
            email: input.value.email,
            roleTitle: input.value.roleTitle,
            ...operatorAuditMetadata(operator.value),
          },
        },
      });
      return c.json(result);
    } catch (error) {
      return brandwellSidekickError(c, error, "AIMEE Sidekick provisioning failed");
    }
  });

  app.post("/internal/sidekicks/:id/:action", async (c) => {
    if (!deps.setSidekickLifecycle) {
      return c.json({ error: "AIMEE Sidekick lifecycle is not configured" }, 503);
    }
    const action = c.req.param("action");
    if (action !== "pause" && action !== "resume" && action !== "cancel") {
      return c.json({ error: "Sidekick action not found" }, 404);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const idempotencyKey = managementIdempotencyKey(c.req.header("x-idempotency-key"));
    if (!idempotencyKey.ok) return c.json({ error: idempotencyKey.error }, 400);
    try {
      const result = await deps.setSidekickLifecycle(c.req.param("id"), action, {
        idempotencyKey: idempotencyKey.value,
        auditMetadata: operatorAuditMetadata(operator.value),
      });
      return c.json(result);
    } catch (error) {
      return brandwellSidekickError(c, error, "AIMEE Sidekick lifecycle update failed");
    }
  });

  app.post("/internal/sidekicks/:id/computer/boot", async (c) => {
    const request = await supportSidekickComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.boot(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.post("/internal/sidekicks/:id/computer/takeover", async (c) => {
    const request = await supportSidekickComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.takeControl(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.get("/internal/sidekicks/:id/computer/screen", async (c) => {
    const request = await supportSidekickComputerRequest(c, deps, c.req.query("reason"));
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.screen(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.post("/internal/sidekicks/:id/computer/release", async (c) => {
    const request = await supportSidekickComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.release(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.post("/internal/workspaces/:id/skills/rollout", async (c) => {
    if (!deps.rolloutSkillBundle) {
      return c.json({ error: "AIMEE skill rollout is not configured" }, 503);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    try {
      const result = await deps.rolloutSkillBundle(c.req.param("id"));
      const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
      if (mapping) {
        await deps.prisma.brandwellAuditLog.create({
          data: {
            workspaceId: mapping.rakazoWorkspaceId,
            actorType: "brandwell_operator",
            action: "skills.rollout",
            resourceType: "brandwell_ai_workspace",
            resourceId: mapping.id,
            metadata: { ...result, ...operatorAuditMetadata(operator.value) },
          },
        });
      }
      return c.json(result);
    } catch (error) {
      return brandwellSidekickError(c, error, "AIMEE skill rollout failed");
    }
  });

  app.get("/internal/workspaces/:id", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    await deps.reconcileModelUsage?.(mapping.rakazoWorkspaceId);
    return c.json(await workspaceDetail(deps.prisma, mapping));
  });

  app.get("/internal/workspaces/:id/binding", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping?.serviceIdentityId) {
      return c.json({ error: "Workspace service identity not found" }, 404);
    }
    return c.json({
      brandwellCustomerId: mapping.brandwellCustomerId,
      workspaceId: mapping.rakazoWorkspaceId,
      serviceIdentityId: mapping.serviceIdentityId,
      subscriptionStatus: mapping.subscriptionStatus,
      provisioningStatus: mapping.provisioningStatus,
    });
  });

  app.post("/internal/workspaces/:id/cancel", async (c) => {
    if (!deps.cancelWorkspace) {
      return c.json({ error: "AIMEE cancellation is not configured" }, 503);
    }
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const body: Record<string, unknown> = await c.req
      .json<Record<string, unknown>>()
      .catch(() => ({}));
    const reason =
      typeof body.reason === "string" && body.reason.trim() ? body.reason.trim() : undefined;
    try {
      const result = await deps.cancelWorkspace(c.req.param("id"), reason);
      await deps.prisma.brandwellAuditLog.create({
        data: {
          workspaceId: mapping.rakazoWorkspaceId,
          actorType: "brandwell_operator",
          action: "workspace.cancel.request",
          resourceType: "brandwell_ai_workspace",
          resourceId: mapping.id,
          metadata: {
            reason: reason ?? null,
            retentionEndsAt: result.retentionEndsAt.toISOString(),
            ...operatorAuditMetadata(operator.value),
          },
        },
      });
      return c.json({
        status: "canceling",
        retentionEndsAt: result.retentionEndsAt.toISOString(),
        executed: result.executed,
      });
    } catch (error) {
      if (error instanceof Error && error.message === "BrandWell workspace not found") {
        return c.json({ error: "Workspace not found" }, 404);
      }
      return c.json({ error: "AIMEE cancellation failed" }, 500);
    }
  });

  app.get("/internal/workspaces/:id/agents", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const agents = await deps.prisma.bot.findMany({
      where: {
        workspaceId: mapping.rakazoWorkspaceId,
        managedByBrandWell: true,
        archivedAt: null,
      },
      orderBy: [{ pinned: "desc" }, { updatedAt: "desc" }],
    });
    return c.json({ agents: agents.map(agentDto) });
  });

  app.get("/internal/workspaces/:id/conversations", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const limit = boundedLimit(c.req.query("limit"));
    const employees = await deps.prisma.bot.findMany({
      where: {
        workspaceId: mapping.rakazoWorkspaceId,
        managedByBrandWell: true,
        archivedAt: null,
      },
      select: { id: true, name: true, title: true },
      orderBy: [{ pinned: "desc" }, { updatedAt: "desc" }],
    });
    const employeeById = new Map(employees.map((employee) => [employee.id, employee]));
    const threads = employees.length
      ? await deps.prisma.thread.findMany({
          where: {
            workspaceId: mapping.rakazoWorkspaceId,
            botId: { in: employees.map((employee) => employee.id) },
            archivedAt: null,
          },
          select: { id: true, botId: true, title: true, updatedAt: true },
          orderBy: [{ updatedAt: "desc" }],
          take: Math.min(limit, 25),
        })
      : [];
    const recent = threads.length
      ? await deps.prisma.message.findMany({
          where: { threadId: { in: threads.map((thread) => thread.id) } },
          select: {
            id: true,
            threadId: true,
            seq: true,
            role: true,
            blocks: true,
            runId: true,
            createdAt: true,
          },
          orderBy: [{ createdAt: "desc" }, { seq: "desc" }],
          take: limit,
        })
      : [];
    const safeMessages = recent
      .map((message) => ({ ...message, text: sanitizedConversationText(message.blocks) }))
      .filter((message) => message.text);
    const conversations = threads
      .map((thread) => ({
        id: thread.id,
        title: thread.title,
        updatedAt: thread.updatedAt,
        employee: thread.botId ? (employeeById.get(thread.botId) ?? null) : null,
        messages: safeMessages
          .filter((message) => message.threadId === thread.id)
          .sort((left, right) => left.seq - right.seq)
          .map((message) => ({
            id: message.id,
            seq: message.seq,
            role: conversationRole(message.role),
            text: message.text,
            runId: message.runId,
            createdAt: message.createdAt,
          })),
      }))
      .filter((conversation) => conversation.messages.length > 0);
    const reason = String(c.req.query("reason") ?? "")
      .trim()
      .slice(0, 500);
    await deps.prisma.brandwellAuditLog.create({
      data: {
        workspaceId: mapping.rakazoWorkspaceId,
        actorType: "brandwell_operator",
        action: "conversation.inspect",
        resourceType: "brandwell_ai_workspace",
        resourceId: mapping.id,
        metadata: {
          conversationCount: conversations.length,
          messageCount: safeMessages.length,
          limit,
          ...(reason ? { reason } : {}),
          ...operatorAuditMetadata(operator.value),
        },
      },
    });
    return c.json({
      conversations,
      messageCount: safeMessages.length,
      limited: recent.length >= limit,
      inspectedAt: new Date().toISOString(),
    });
  });

  app.get("/internal/workspaces/:id/computer", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const computer = await deps.prisma.computer.findFirst({
      where: { workspaceId: mapping.rakazoWorkspaceId, scope: "team" },
      orderBy: [{ updatedAt: "desc" }],
    });
    return c.json({ computer: computer ? computerDto(computer) : null });
  });

  app.post("/internal/workspaces/:id/computer/boot", async (c) => {
    const request = await supportComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.boot(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.post("/internal/workspaces/:id/computer/takeover", async (c) => {
    const request = await supportComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.takeControl(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.get("/internal/workspaces/:id/computer/screen", async (c) => {
    const request = await supportComputerRequest(c, deps, c.req.query("reason"));
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.screen(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.post("/internal/workspaces/:id/computer/release", async (c) => {
    const request = await supportComputerRequest(c, deps);
    if (!request.ok) return c.json({ error: request.error }, request.status);
    try {
      return c.json(await deps.computerSupport!.release(request.value));
    } catch (error) {
      return supportComputerError(c, error);
    }
  });

  app.get("/internal/workspaces/:id/runs", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const runs = await deps.prisma.run.findMany({
      where: { workspaceId: mapping.rakazoWorkspaceId },
      orderBy: [{ createdAt: "desc" }],
      take: boundedLimit(c.req.query("limit")),
    });
    return c.json({ runs });
  });

  app.get("/internal/workspaces/:id/routines", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const routines = await deps.prisma.routine.findMany({
      where: { workspaceId: mapping.rakazoWorkspaceId },
      orderBy: [{ active: "desc" }, { nextRunAt: "asc" }],
    });
    return c.json({ routines });
  });

  app.get("/internal/workspaces/:id/alerts", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const alerts = await deps.prisma.brandwellAlert.findMany({
      where: { workspaceId: mapping.rakazoWorkspaceId },
      orderBy: [{ resolvedAt: "asc" }, { createdAt: "desc" }],
      take: boundedLimit(c.req.query("limit")),
    });
    return c.json({ alerts });
  });

  app.get("/internal/workspaces/:id/usage", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    await deps.reconcileModelUsage?.(mapping.rakazoWorkspaceId);
    return c.json(await usageDto(deps.prisma, mapping.rakazoWorkspaceId));
  });

  app.get("/internal/workspaces/:id/integrations", async (c) => {
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const integrations = await deps.prisma.connection.findMany({
      where: {
        workspaceId: mapping.rakazoWorkspaceId,
        ownerType: "service",
      },
      orderBy: [{ status: "asc" }, { displayName: "asc" }],
    });
    return c.json({ integrations: integrations.map(integrationDto) });
  });

  app.post("/internal/bots/:id/pause", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const bot = await deps.prisma.bot.findFirst({
      where: { id: c.req.param("id"), managedByBrandWell: true, archivedAt: null },
      select: { id: true, workspaceId: true },
    });
    if (!bot) return c.json({ error: "AI employee not found" }, 404);
    await deps.prisma.$transaction([
      deps.prisma.bot.update({ where: { id: bot.id }, data: { managedStatus: "paused" } }),
      deps.prisma.routine.updateMany({ where: { botId: bot.id }, data: { active: false } }),
      deps.prisma.brandwellAuditLog.create({
        data: {
          workspaceId: bot.workspaceId,
          actorType: "brandwell_operator",
          action: "employee.pause",
          resourceType: "bot",
          resourceId: bot.id,
          metadata: operatorAuditMetadata(operator.value),
        },
      }),
    ]);
    return c.json({ ok: true, status: "paused" });
  });

  app.post("/internal/bots/:id/resume", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const bot = await deps.prisma.bot.findFirst({
      where: { id: c.req.param("id"), managedByBrandWell: true, archivedAt: null },
      select: { id: true, workspaceId: true },
    });
    if (!bot) return c.json({ error: "AI employee not found" }, 404);
    await deps.prisma.$transaction([
      deps.prisma.bot.update({ where: { id: bot.id }, data: { managedStatus: "active" } }),
      deps.prisma.brandwellAuditLog.create({
        data: {
          workspaceId: bot.workspaceId,
          actorType: "brandwell_operator",
          action: "employee.resume",
          resourceType: "bot",
          resourceId: bot.id,
          metadata: operatorAuditMetadata(operator.value),
        },
      }),
    ]);
    return c.json({ ok: true, status: "active" });
  });

  app.post("/internal/bots/:id/instructions", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = employeeInstructionsInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const bot = await deps.prisma.bot.findFirst({
      where: { id: c.req.param("id"), managedByBrandWell: true, archivedAt: null },
      select: { id: true, workspaceId: true },
    });
    if (!bot) return c.json({ error: "AI employee not found" }, 404);
    const updated = await deps.prisma.$transaction(async (tx) => {
      const row = await tx.bot.update({
        where: { id: bot.id },
        data: { instructions: input.value.instructions },
        select: { id: true, instructions: true, updatedAt: true },
      });
      await tx.brandwellAuditLog.create({
        data: {
          workspaceId: bot.workspaceId,
          actorType: "brandwell_operator",
          action: "employee.instructions.update",
          resourceType: "bot",
          resourceId: bot.id,
          metadata: {
            characterCount: row.instructions.length,
            ...operatorAuditMetadata(operator.value),
          },
        },
      });
      return row;
    });
    return c.json({
      ok: true,
      employeeId: updated.id,
      instructions: updated.instructions,
      updatedAt: updated.updatedAt,
    });
  });

  app.post("/internal/routines/:id/settings", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = routineSettingsInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const scheduleMutation =
      input.value.crons !== undefined ||
      input.value.timezone !== undefined ||
      input.value.active !== undefined;
    if (scheduleMutation && !deps.jobs) {
      return c.json({ error: "AIMEE routine scheduling is not configured" }, 503);
    }
    const existing = await deps.prisma.routine.findFirst({
      where: {
        id: c.req.param("id"),
        bot: { managedByBrandWell: true, archivedAt: null },
      },
      include: { bot: { select: { managedStatus: true } } },
    });
    if (!existing) return c.json({ error: "AIMEE routine not found" }, 404);
    const active = input.value.active ?? existing.active;
    const crons = input.value.crons ?? existing.crons;
    const timezone = input.value.timezone ?? existing.timezone;
    if (active && existing.bot.managedStatus !== "active") {
      return c.json({ error: "Resume the AI employee before activating a routine" }, 409);
    }
    if (active) {
      const mapping = await deps.prisma.brandwellAiWorkspace.findUnique({
        where: { rakazoWorkspaceId: existing.workspaceId },
        select: { subscriptionStatus: true },
      });
      if (!mapping || !["trialing", "active"].includes(mapping.subscriptionStatus)) {
        return c.json({ error: "The AIMEE subscription is not active" }, 409);
      }
    }
    const scheduleChanged =
      input.value.active !== undefined ||
      input.value.timezone !== undefined ||
      input.value.crons !== undefined;
    const schedule = managedRoutineSchedule(crons, timezone);
    if (!schedule.ok) return c.json({ error: schedule.error }, 400);
    const nextRunAt = active ? schedule.nextRunAt : null;
    const updated = await deps.prisma.$transaction(async (tx) => {
      const row = await tx.routine.update({
        where: { id: existing.id },
        data: {
          name: input.value.name,
          prompt: input.value.prompt,
          crons: input.value.crons,
          timezone: input.value.timezone,
          active: input.value.active,
          notify: input.value.notify,
          ...(scheduleChanged ? { nextRunAt } : {}),
        },
      });
      await tx.brandwellAuditLog.create({
        data: {
          workspaceId: existing.workspaceId,
          actorType: "brandwell_operator",
          action: "routine.settings.update",
          resourceType: "routine",
          resourceId: existing.id,
          metadata: {
            active: row.active,
            timezone: row.timezone,
            schedules: row.crons.length,
            ...operatorAuditMetadata(operator.value),
          },
        },
      });
      return row;
    });
    if (scheduleChanged) {
      if (updated.active && updated.nextRunAt) {
        await deps.jobs!.enqueue(routineWakeupJob(updated.id, updated.nextRunAt));
      } else {
        await deps.jobs!.cancel(routineJobKey(updated.id));
      }
    }
    return c.json({ ok: true, routine: routineDto(updated) });
  });

  app.post("/internal/workspaces/:id/model-policy", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = modelPolicyInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const policyLease = await acquireBrandwellModelPolicyLease(
      deps.prisma,
      mapping.id,
      "model-policy-update",
    );
    if (!policyLease) {
      return c.json(
        { error: "Another model policy or Sidekick change is already in progress" },
        409,
      );
    }
    try {
      const current = await deps.prisma.brandwellWorkspaceModelCredential.findUnique({
        where: { workspaceId: mapping.rakazoWorkspaceId },
      });
      if (!current) return c.json({ error: "AIMEE model policy is not provisioned" }, 409);
      const limits = resolvedModelLimits(current, input.value);
      if (!limits.ok) return c.json({ error: limits.error }, 400);
      const sidekickCredentials = await deps.prisma.brandwellSidekickModelCredential.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId },
      });
      const platformDefaultModel = await brandwellPlatformModelDefault(deps.prisma);
      const nextPolicy = resolvedModelPolicy(current, input.value, platformDefaultModel);
      let nextModelCatalog = modelCatalogRecord(current.modelCatalog);
      const changedModelIds = modelIdsFromPatch(input.value);
      const configuredModelIds = [
        nextPolicy.preferredModel,
        nextPolicy.computerModel,
        nextPolicy.lightweightModel,
        nextPolicy.reasoningModel,
        ...nextPolicy.fallbackModels,
      ].filter((modelId): modelId is string => Boolean(modelId));
      const modelIdsToValidate = [
        ...new Set([
          ...changedModelIds,
          ...configuredModelIds.filter((modelId) => !nextModelCatalog[modelId]),
        ]),
      ];
      if (modelIdsToValidate.length > 0) {
        if (!deps.validateOpenRouterModel) {
          return c.json({ error: "OpenRouter model validation is not configured" }, 503);
        }
        try {
          const supported = await Promise.all(
            modelIdsToValidate.map((modelId) => deps.validateOpenRouterModel!(modelId)),
          );
          const unsupportedIndex = supported.findIndex((value) => !value);
          if (unsupportedIndex >= 0) {
            return c.json(
              {
                error: `${modelIdsToValidate[unsupportedIndex]} is unavailable or does not support AIMEE tools`,
              },
              400,
            );
          }
          nextModelCatalog = {
            ...nextModelCatalog,
            ...Object.fromEntries(
              supported.map((metadata, index) => [modelIdsToValidate[index], metadata!]),
            ),
          };
        } catch {
          return c.json({ error: "OpenRouter model validation is temporarily unavailable" }, 503);
        }
      }
      if (nextPolicy.computerModel) {
        let computerMetadata = nextModelCatalog[nextPolicy.computerModel];
        if (!computerMetadata) {
          if (!deps.validateOpenRouterModel) {
            return c.json({ error: "OpenRouter model validation is not configured" }, 503);
          }
          try {
            computerMetadata =
              (await deps.validateOpenRouterModel(nextPolicy.computerModel)) ?? undefined;
          } catch {
            return c.json({ error: "OpenRouter model validation is temporarily unavailable" }, 503);
          }
          if (!computerMetadata) {
            return c.json(
              {
                error: `${nextPolicy.computerModel} is unavailable or does not support AIMEE tools`,
              },
              400,
            );
          }
          nextModelCatalog = {
            ...nextModelCatalog,
            [nextPolicy.computerModel]: computerMetadata,
          };
        }
        if (!computerMetadata.inputModalities.includes("image")) {
          return c.json({ error: "The managed computer model must support image input" }, 400);
        }
      }
      const limitTargets = [current, ...sidekickCredentials].filter(
        (credential) => credential.monthlyLimitMicros !== nextPolicy.monthlyLimitMicros,
      );
      if (limitTargets.length > 0) {
        if (limitTargets.some((credential) => !credential.externalKeyHash)) {
          return c.json(
            { error: "Every managed OpenRouter child key must be linked before changing limits" },
            409,
          );
        }
        if (!deps.updateOpenRouterLimit) {
          return c.json({ error: "OpenRouter limit management is not configured" }, 503);
        }
        const attemptedTargets: typeof limitTargets = [];
        try {
          for (const credential of limitTargets) {
            // A timed-out PATCH is ambiguous: OpenRouter may have applied it even
            // though no response arrived. Include the target in compensating work
            // before awaiting the request so rollback covers that case.
            attemptedTargets.push(credential);
            await policyLease.renew();
            await deps.updateOpenRouterLimit(
              credential.externalKeyHash!,
              nextPolicy.monthlyLimitMicros,
            );
          }
        } catch {
          const rollback = await Promise.allSettled(
            [...attemptedTargets]
              .reverse()
              .map((credential) =>
                deps.updateOpenRouterLimit!(
                  credential.externalKeyHash!,
                  credential.monthlyLimitMicros,
                ),
              ),
          );
          if (rollback.some((result) => result.status === "rejected")) {
            await markModelPolicyProviderError(
              deps.prisma,
              mapping.rakazoWorkspaceId,
              "OpenRouter limit rollback failed. Reconcile managed keys before the next policy change.",
            );
            return c.json(
              {
                error:
                  "OpenRouter limit changes need reconciliation before policy updates can continue",
              },
              503,
            );
          }
          return c.json({ error: "OpenRouter rejected the usage-limit update" }, 503);
        }
      }
      let updated: NonNullable<typeof current>;
      try {
        await policyLease.renew();
        updated = await deps.prisma.$transaction(async (tx) => {
          const sharedPolicyData = {
            limitReset: "monthly",
            preferredModel: nextPolicy.preferredModel,
            computerModel: nextPolicy.computerModel,
            lightweightModel: nextPolicy.lightweightModel,
            reasoningModel: nextPolicy.reasoningModel,
            fallbackModels: nextPolicy.fallbackModels,
            modelCatalog: nextModelCatalog,
            maxTokens: nextPolicy.maxTokens,
            thinkingLevel: nextPolicy.thinkingLevel,
            monthlyLimitMicros: nextPolicy.monthlyLimitMicros,
            dailyLimitMicros: nextPolicy.dailyLimitMicros,
            warningLimitMicros: nextPolicy.warningLimitMicros,
          };
          const unknownProviderPolicy = {
            providerLimitMicros: null,
            providerLimitReset: null,
            providerIncludeByokInLimit: null,
          };
          const masterLimitUpdated = limitTargets.some(
            (credential) => credential.id === current.id,
          );
          const row = await tx.brandwellWorkspaceModelCredential.update({
            where: { id: current.id },
            data: {
              ...sharedPolicyData,
              inheritsPlatformModelDefault: nextPolicy.inheritsPlatformModelDefault,
              ...(masterLimitUpdated ? unknownProviderPolicy : {}),
            },
          });
          await tx.brandwellSidekickModelCredential.updateMany({
            where: { workspaceId: mapping.rakazoWorkspaceId },
            data: sharedPolicyData,
          });
          const sidekickLimitTargetIds = limitTargets
            .filter((credential) => credential.id !== current.id)
            .map((credential) => credential.id);
          if (sidekickLimitTargetIds.length > 0) {
            await tx.brandwellSidekickModelCredential.updateMany({
              where: { id: { in: sidekickLimitTargetIds } },
              data: unknownProviderPolicy,
            });
          }
          await tx.bot.updateMany({
            where: {
              workspaceId: mapping.rakazoWorkspaceId,
              managedByBrandWell: true,
              archivedAt: null,
            },
            data: {
              modelProvider: row.provider,
              modelId: row.preferredModel,
              thinkingLevel: row.thinkingLevel,
            },
          });
          await tx.brandwellAuditLog.create({
            data: {
              workspaceId: mapping.rakazoWorkspaceId,
              actorType: "brandwell_operator",
              action: "model.policy.update",
              resourceType: "model_policy",
              resourceId: row.id,
              metadata: {
                preferredModel: row.preferredModel,
                inheritsPlatformModelDefault: row.inheritsPlatformModelDefault,
                computerModel: row.computerModel,
                lightweightModel: row.lightweightModel,
                reasoningModel: row.reasoningModel,
                fallbackModels: stringArray(row.fallbackModels),
                monthlyLimitMicros: row.monthlyLimitMicros.toString(),
                sidekickCredentialsUpdated: sidekickCredentials.length,
                openRouterLimitsUpdated: limitTargets.length,
                ...operatorAuditMetadata(operator.value),
              },
            },
          });
          return row;
        });
      } catch {
        const rollback = await Promise.allSettled(
          limitTargets.map((credential) =>
            deps.updateOpenRouterLimit!(credential.externalKeyHash!, credential.monthlyLimitMicros),
          ),
        );
        if (rollback.some((result) => result.status === "rejected")) {
          await markModelPolicyProviderError(
            deps.prisma,
            mapping.rakazoWorkspaceId,
            "The policy database update failed and OpenRouter limits need reconciliation.",
          );
        }
        return c.json({ error: "AIMEE could not save the centralized model policy" }, 503);
      }
      return c.json({
        ok: true,
        modelPolicy: modelPolicyDto(updated),
        managedCredentialsUpdated: sidekickCredentials.length + 1,
        sidekickCredentialsUpdated: sidekickCredentials.length,
        openRouterLimitsUpdated: limitTargets.length,
      });
    } finally {
      await policyLease.release().catch(() => undefined);
    }
  });

  app.post("/internal/alerts/:id/status", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = alertStatusInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const alert = await deps.prisma.brandwellAlert.findUnique({
      where: { id: c.req.param("id") },
    });
    if (!alert) return c.json({ error: "AIMEE alert not found" }, 404);
    const now = new Date();
    const updated = await deps.prisma.$transaction(async (tx) => {
      const row = await tx.brandwellAlert.update({
        where: { id: alert.id },
        data: {
          status: input.value.status,
          acknowledgedAt: input.value.status === "OPEN" ? null : (alert.acknowledgedAt ?? now),
          resolvedAt: ["RESOLVED", "IGNORED"].includes(input.value.status) ? now : null,
        },
      });
      await tx.brandwellAuditLog.create({
        data: {
          workspaceId: alert.workspaceId,
          actorType: "brandwell_operator",
          action: `alert.${input.value.status.toLowerCase()}`,
          resourceType: "alert",
          resourceId: alert.id,
          metadata: operatorAuditMetadata(operator.value),
        },
      });
      return row;
    });
    return c.json({ ok: true, alert: updated });
  });

  app.post("/internal/bots/:id/run", async (c) => {
    if (!deps.jobs) return c.json({ error: "AIMEE job execution is not configured" }, 503);
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const requestKey = managementIdempotencyKey(c.req.header("x-idempotency-key"));
    if (!requestKey.ok) return c.json({ error: requestKey.error }, 400);
    const bot = await deps.prisma.bot.findFirst({
      where: {
        id: c.req.param("id"),
        managedByBrandWell: true,
        managedStatus: "active",
        archivedAt: null,
        workspace: {
          brandwellWorkspace: { subscriptionStatus: { in: ["trialing", "active"] } },
        },
      },
      select: {
        id: true,
        workspaceId: true,
        userId: true,
        serviceIdentityId: true,
        thread: { select: { id: true } },
      },
    });
    if (!bot?.thread || !bot.serviceIdentityId) {
      return c.json({ error: "Active AI employee not found" }, 404);
    }
    const threadId = bot.thread.id;
    const routine = await deps.prisma.routine.findFirst({
      where: { botId: bot.id, workspaceId: bot.workspaceId, active: true },
      orderBy: [{ nextRunAt: "asc" }, { createdAt: "asc" }],
    });
    if (!routine) return c.json({ error: "No active routine is available to run" }, 409);
    const clientNonce = `brandwell-support:${requestKey.value}`;
    const existing = await deps.prisma.run.findFirst({
      where: { workspaceId: bot.workspaceId, clientNonce },
      select: { id: true, status: true },
    });
    if (existing) return c.json({ runId: existing.id, status: existing.status, replayed: true });
    let run: { id: string; status: string; replayed: boolean };
    try {
      run = await deps.prisma.$transaction(async (tx) => {
        const replay = await tx.run.findFirst({
          where: { workspaceId: bot.workspaceId, clientNonce },
          select: { id: true, status: true },
        });
        if (replay) return { ...replay, replayed: true };
        const task = await tx.task.create({
          data: {
            workspaceId: bot.workspaceId,
            botId: bot.id,
            threadId,
            userId: bot.userId,
            prompt: routine.prompt,
            status: "queued",
          },
        });
        const created = await tx.run.create({
          data: {
            workspaceId: bot.workspaceId,
            botId: bot.id,
            threadId,
            taskId: task.id,
            userId: bot.userId,
            serviceIdentityId: bot.serviceIdentityId,
            routineId: routine.id,
            status: "queued",
            trigger: "brandwell_support",
            clientNonce,
          },
          select: { id: true, status: true },
        });
        await tx.brandwellAuditLog.create({
          data: {
            workspaceId: bot.workspaceId,
            actorType: "brandwell_operator",
            action: "employee.run_now",
            resourceType: "run",
            resourceId: created.id,
            metadata: { routineId: routine.id, ...operatorAuditMetadata(operator.value) },
          },
        });
        return { ...created, replayed: false };
      });
    } catch (error) {
      const replay = await deps.prisma.run.findFirst({
        where: { workspaceId: bot.workspaceId, clientNonce },
        select: { id: true, status: true },
      });
      if (!replay) throw error;
      run = { ...replay, replayed: true };
    }
    await deps.jobs.enqueue(runContinueJob(run.id)).catch(() => undefined);
    return c.json({ runId: run.id, status: run.status, replayed: run.replayed });
  });

  app.post("/internal/runs/:id/retry", async (c) => {
    if (!deps.jobs) return c.json({ error: "AIMEE job execution is not configured" }, 503);
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const run = await deps.prisma.run.findFirst({
      where: {
        id: c.req.param("id"),
        status: "failed",
        bot: {
          managedByBrandWell: true,
          managedStatus: "active",
          archivedAt: null,
          workspace: {
            brandwellWorkspace: { subscriptionStatus: { in: ["trialing", "active"] } },
          },
        },
      },
      select: { id: true, workspaceId: true, taskId: true, botId: true },
    });
    if (!run) return c.json({ error: "Failed AIMEE run not found" }, 404);
    const reset = await deps.prisma.$transaction(async (tx) => {
      const updated = await tx.run.updateMany({
        where: { id: run.id, status: "failed" },
        data: {
          status: "queued",
          error: null,
          startedAt: null,
          completedAt: null,
          leaseOwner: null,
          leaseExpiresAt: null,
          checkpoint: null,
        },
      });
      if (updated.count !== 1) return false;
      await tx.task.update({ where: { id: run.taskId }, data: { status: "queued" } });
      await tx.brandwellAuditLog.create({
        data: {
          workspaceId: run.workspaceId,
          actorType: "brandwell_operator",
          action: "run.retry",
          resourceType: "run",
          resourceId: run.id,
          metadata: { botId: run.botId, ...operatorAuditMetadata(operator.value) },
        },
      });
      return true;
    });
    if (!reset) return c.json({ error: "Run is no longer retryable" }, 409);
    await deps.jobs.enqueue(runContinueJob(run.id)).catch(() => undefined);
    return c.json({ ok: true, runId: run.id, status: "queued" });
  });

  app.post("/internal/workspaces/:id/outreach-followup", async (c) => {
    if (!deps.jobs) return c.json({ error: "AIMEE job execution is not configured" }, 503);
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const key = managementIdempotencyKey(c.req.header("x-idempotency-key"));
    if (!key.ok) return c.json({ error: key.error }, 400);
    const input = parseBrandwellOutreachFollowup(await c.req.json().catch(() => null));
    if (!input.ok) return c.json({ error: input.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (
      !mapping ||
      !["active", "trialing"].includes(mapping.subscriptionStatus) ||
      !["active", "trialing"].includes(mapping.commercialStatus)
    ) {
      return c.json({ error: "Active AIMEE workspace not found" }, 404);
    }
    const user = await deps.prisma.user.findFirst({
      where: {
        brandwellUserId: input.value.targetBrandwellUserId,
        members: { some: { organizationId: mapping.rakazoWorkspaceId } },
      },
      select: { id: true },
    });
    if (!user)
      return c.json({ error: "The assigned user must have current AIMEE workspace access" }, 409);
    let botId =
      mapping.primaryBrandwellUserId === input.value.targetBrandwellUserId
        ? mapping.primaryBotId
        : null;
    if (!botId) {
      const seat = await deps.prisma.brandwellSidekick.findFirst({
        where: {
          aiWorkspaceId: mapping.id,
          workspaceId: mapping.rakazoWorkspaceId,
          brandwellUserId: input.value.targetBrandwellUserId,
          userId: user.id,
          status: "active",
        },
        select: { botId: true },
      });
      botId = seat?.botId ?? null;
    }
    if (!botId) return c.json({ error: "The assigned user has no active AI employee" }, 409);
    const bot = await deps.prisma.bot.findFirst({
      where: {
        id: botId,
        workspaceId: mapping.rakazoWorkspaceId,
        managedByBrandWell: true,
        managedStatus: "active",
        archivedAt: null,
      },
      select: {
        id: true,
        workspaceId: true,
        userId: true,
        serviceIdentityId: true,
        thread: { select: { id: true } },
      },
    });
    if (!bot?.thread || !bot.serviceIdentityId)
      return c.json({ error: "The AI employee is not ready" }, 409);
    const prompt = brandwellOutreachFollowupPrompt(input.value);
    const clientNonce = `brandwell-outreach:${key.value}`;
    const where = { workspaceId: bot.workspaceId, clientNonce };
    const select = {
      id: true,
      botId: true,
      taskId: true,
      status: true,
      task: { select: { prompt: true } },
    } as const;
    let run = await deps.prisma.run.findFirst({ where, select });
    let replayed = Boolean(run);
    if (!run) {
      try {
        run = await deps.prisma.$transaction(async (tx) => {
          const prior = await tx.run.findFirst({ where, select });
          if (prior) {
            replayed = true;
            return prior;
          }
          const task = await tx.task.create({
            data: {
              workspaceId: bot.workspaceId,
              botId: bot.id,
              threadId: bot.thread!.id,
              userId: bot.userId,
              prompt,
              status: "queued",
            },
          });
          const created = await tx.run.create({
            data: {
              workspaceId: bot.workspaceId,
              botId: bot.id,
              threadId: bot.thread!.id,
              userId: bot.userId,
              taskId: task.id,
              serviceIdentityId: bot.serviceIdentityId,
              status: "queued",
              trigger: input.value.socialSignal
                ? "brandwell_socialstreams_review"
                : input.value.mode === "execute"
                  ? "brandwell_outreach_action"
                  : "brandwell_outreach_review",
              clientNonce,
            },
            select,
          });
          await tx.brandwellAuditLog.create({
            data: {
              workspaceId: bot.workspaceId,
              actorType: "brandwell_operator",
              action:
                input.value.mode === "execute"
                  ? "outreach.followup.execute"
                  : "outreach.followup.prepare",
              resourceType: "run",
              resourceId: created.id,
              metadata: {
                targetBrandwellUserId: input.value.targetBrandwellUserId,
                ...operatorAuditMetadata(operator.value),
              },
            },
          });
          return created;
        });
      } catch (error) {
        run = await deps.prisma.run.findFirst({ where, select });
        if (!run) throw error;
        replayed = true;
      }
    }
    if (run.botId !== bot.id || run.task.prompt !== prompt)
      return c.json(
        { error: "This follow-up identity was already used with different details" },
        409,
      );
    if (run.status === "queued") {
      try {
        await deps.jobs.enqueue(runContinueJob(run.id));
      } catch {
        return c.json(
          {
            error: "The task is saved but its worker dispatch needs a retry",
            runId: run.id,
            accepted: true,
          },
          503,
        );
      }
    }
    return c.json({
      taskId: run.taskId,
      runId: run.id,
      botId: run.botId,
      status: run.status,
      replayed,
    });
  });

  app.post("/internal/workspaces/:id/notify-client", async (c) => {
    const operator = supportActor(c.req.header());
    if (!operator.ok) return c.json({ error: operator.error }, 400);
    const mapping = await findWorkspaceMapping(deps.prisma, c.req.param("id"));
    if (!mapping) return c.json({ error: "Workspace not found" }, 404);
    const requestKey = managementIdempotencyKey(c.req.header("x-idempotency-key"));
    if (!requestKey.ok) return c.json({ error: requestKey.error }, 400);
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    const input = clientNotificationInput(body);
    if (!input.ok) return c.json({ error: input.error }, 400);
    const targets = input.value.targetBrandwellUserIds
      ? await deps.prisma.user.findMany({
          where: {
            brandwellUserId: { in: input.value.targetBrandwellUserIds },
            members: { some: { organizationId: mapping.rakazoWorkspaceId } },
          },
          select: { id: true, brandwellUserId: true },
        })
      : [];
    if (
      input.value.targetBrandwellUserIds &&
      targets.length !== input.value.targetBrandwellUserIds.length
    ) {
      return c.json(
        { error: "Every notification recipient must be a current AIMEE workspace member" },
        409,
      );
    }
    const dedupeKey = `brandwell-notify:${requestKey.value}`;
    const existing = await deps.prisma.brandwellClientNotification.findUnique({
      where: {
        workspaceId_dedupeKey: { workspaceId: mapping.rakazoWorkspaceId, dedupeKey },
      },
    });
    if (existing) return c.json(clientNotificationResponse(existing, true));
    let row: ClientNotificationRecord;
    try {
      row = await deps.prisma.$transaction(async (tx) => {
        const notification = await tx.brandwellClientNotification.create({
          data: {
            workspaceId: mapping.rakazoWorkspaceId,
            dedupeKey,
            type: input.value.type,
            title: input.value.title,
            body: input.value.body,
            severity: input.value.severity,
            requiresAction: input.value.requiresAction,
            targetUserIds: targets.map((target) => target.id),
            actionType: input.value.actionType,
            actionTarget: input.value.actionTarget,
          },
        });
        await tx.brandwellAuditLog.create({
          data: {
            workspaceId: mapping.rakazoWorkspaceId,
            actorType: "brandwell_operator",
            action: "client.notify",
            resourceType: "notification",
            resourceId: notification.id,
            metadata: {
              type: notification.type,
              requiresAction: notification.requiresAction,
              ...operatorAuditMetadata(operator.value),
            },
          },
        });
        return notification;
      });
    } catch (error) {
      const replay = await deps.prisma.brandwellClientNotification.findUnique({
        where: {
          workspaceId_dedupeKey: { workspaceId: mapping.rakazoWorkspaceId, dedupeKey },
        },
      });
      if (!replay) throw error;
      return c.json(clientNotificationResponse(replay, true));
    }
    return c.json(clientNotificationResponse(row, false));
  });
}

function conversationRole(role: string): "client" | "aimee" | "system" {
  const normalized = role.trim().toLowerCase();
  if (["user", "human", "client"].includes(normalized)) return "client";
  if (["assistant", "bot", "aimee"].includes(normalized)) return "aimee";
  return "system";
}

export function sanitizedConversationText(value: unknown): string {
  if (!Array.isArray(value)) return "";
  const text: string[] = [];
  for (const candidate of value) {
    if (!candidate || typeof candidate !== "object") continue;
    const block = candidate as Record<string, unknown>;
    const kind = String(block.kind ?? "");
    if (["text", "meta", "progress", "computer", "handoff"].includes(kind)) {
      appendSafeConversationText(text, block.text);
    } else if (kind === "ask") {
      appendSafeConversationText(text, block.text);
    } else if (kind === "choice") {
      appendSafeConversationText(text, block.question);
    } else if (kind === "bot_message_sent") {
      appendSafeConversationText(text, block.text, `To ${String(block.toBotName ?? "teammate")}: `);
    } else if (kind === "bot_message_received") {
      appendSafeConversationText(
        text,
        block.text,
        `From ${String(block.fromBotName ?? "teammate")}: `,
      );
    } else if (kind === "card" && Array.isArray(block.lines)) {
      for (const line of block.lines) {
        if (!line || typeof line !== "object") continue;
        const item = line as Record<string, unknown>;
        const key = compactConversationText(item.k, 120);
        const detail = compactConversationText(item.v, 500);
        if (key && detail) text.push(`${key}: ${detail}`);
      }
    } else if (kind === "subagent") {
      appendSafeConversationText(text, block.progress);
      appendSafeConversationText(text, block.result);
    }
  }
  return text.join("\n").split(String.fromCharCode(0)).join("").trim().slice(0, 4_000);
}

function appendSafeConversationText(target: string[], value: unknown, prefix = ""): void {
  const compact = compactConversationText(value, 4_000);
  if (compact) target.push(`${prefix}${compact}`);
}

function compactConversationText(value: unknown, limit: number): string {
  return typeof value === "string"
    ? value
        .split(String.fromCharCode(0))
        .join("")
        .replace(/[ \t]+/g, " ")
        .trim()
        .slice(0, limit)
    : "";
}

async function findWorkspaceMapping(prisma: PrismaClient, id: string) {
  return prisma.brandwellAiWorkspace.findFirst({
    where: {
      OR: [{ id }, { brandwellCustomerId: id }, { rakazoWorkspaceId: id }],
    },
    include: { rakazoWorkspace: { select: { name: true, slug: true } } },
  });
}

async function fleetRow(prisma: PrismaClient, mapping: NonNullable<WorkspaceMapping>) {
  return (await fleetRows(prisma, [mapping]))[0]!;
}

/** Load an entire Super Admin page with a bounded set of batched queries. */
async function fleetRows(prisma: PrismaClient, mappings: NonNullable<WorkspaceMapping>[]) {
  if (mappings.length === 0) return [];
  const workspaceIds = mappings.map((mapping) => mapping.rakazoWorkspaceId);
  const mappingIds = mappings.map((mapping) => mapping.id);
  const [agents, computers, runs, routines, alertGroups, usageGroups, credentials, sidekicks] =
    await Promise.all([
      prisma.bot.findMany({
        where: {
          workspaceId: { in: workspaceIds },
          managedByBrandWell: true,
          archivedAt: null,
        },
        orderBy: [{ workspaceId: "asc" }, { pinned: "desc" }, { updatedAt: "desc" }],
      }),
      prisma.computer.findMany({
        where: { workspaceId: { in: workspaceIds }, scope: "team" },
        orderBy: [{ workspaceId: "asc" }, { updatedAt: "desc" }],
        distinct: ["workspaceId"],
      }),
      prisma.run.findMany({
        where: { workspaceId: { in: workspaceIds } },
        orderBy: [{ workspaceId: "asc" }, { createdAt: "desc" }],
        distinct: ["workspaceId"],
      }),
      prisma.routine.findMany({
        where: { workspaceId: { in: workspaceIds }, active: true },
        orderBy: [{ workspaceId: "asc" }, { nextRunAt: "asc" }],
        distinct: ["workspaceId"],
      }),
      prisma.brandwellAlert.groupBy({
        by: ["workspaceId"],
        where: {
          workspaceId: { in: workspaceIds },
          status: { notIn: ["RESOLVED", "IGNORED"] },
        },
        _count: { id: true },
      }),
      prisma.usageRecord.groupBy({
        by: ["workspaceId"],
        where: { workspaceId: { in: workspaceIds } },
        _sum: { inputTokens: true, outputTokens: true, costMicros: true },
        _count: { id: true },
      }),
      prisma.brandwellWorkspaceModelCredential.findMany({
        where: { workspaceId: { in: workspaceIds } },
        select: {
          workspaceId: true,
          status: true,
          monthlyLimitMicros: true,
          warningLimitMicros: true,
          currentUsageMicros: true,
          providerLimitMicros: true,
          providerUsageSyncedAt: true,
          providerUsageSyncError: true,
          preferredModel: true,
          disabledAt: true,
        },
      }),
      prisma.brandwellSidekick.findMany({
        where: { aiWorkspaceId: { in: mappingIds } },
        include: { bot: true, computer: true, modelCredential: true },
        orderBy: [{ aiWorkspaceId: "asc" }, { createdAt: "asc" }, { id: "asc" }],
      }),
    ]);
  const sidekickStats = await sidekickOperationalStats(prisma, sidekicks);

  const firstByWorkspace = <T extends { workspaceId: string }>(rows: T[]) => {
    const result = new Map<string, T>();
    for (const row of rows) if (!result.has(row.workspaceId)) result.set(row.workspaceId, row);
    return result;
  };
  const agentsByWorkspace = firstByWorkspace(agents);
  const agentsById = new Map(agents.map((agent) => [agent.id, agent]));
  const computersByWorkspace = firstByWorkspace(computers);
  const runsByWorkspace = firstByWorkspace(runs);
  const routinesByWorkspace = firstByWorkspace(routines);
  const alertsByWorkspace = new Map(alertGroups.map((row) => [row.workspaceId, row._count.id]));
  const usageByWorkspace = new Map(usageGroups.map((row) => [row.workspaceId, row]));
  const credentialsByWorkspace = new Map(
    credentials.map((credential) => [credential.workspaceId, credential]),
  );
  const sidekicksByMapping = new Map<string, typeof sidekicks>();
  for (const sidekick of sidekicks) {
    const rows = sidekicksByMapping.get(sidekick.aiWorkspaceId) ?? [];
    rows.push(sidekick);
    sidekicksByMapping.set(sidekick.aiWorkspaceId, rows);
  }

  return mappings.map((mapping) => {
    const workspaceId = mapping.rakazoWorkspaceId;
    const agent =
      (mapping.primaryBotId ? agentsById.get(mapping.primaryBotId) : undefined) ??
      agentsByWorkspace.get(workspaceId);
    const computer = computersByWorkspace.get(workspaceId);
    const usage = usageByWorkspace.get(workspaceId);
    const credential = credentialsByWorkspace.get(workspaceId);
    const mappingSidekicks = sidekicksByMapping.get(mapping.id) ?? [];
    return {
      id: mapping.id,
      brandwellCustomerId: mapping.brandwellCustomerId,
      workspaceId,
      client: mapping.rakazoWorkspace.name,
      slug: mapping.rakazoWorkspace.slug,
      subscriptionStatus: mapping.subscriptionStatus,
      entitlement: {
        agencyId: mapping.brandwellAgencyId,
        clientId: mapping.brandwellClientId,
        contractId: mapping.brandwellContractId,
        revision: mapping.commercialRevision.toString(),
        status: mapping.commercialStatus,
        masterSeats: mapping.masterSeats,
        sidekickSeats: mapping.sidekickSeats,
        skillBundleVersion: mapping.skillBundleVersion,
      },
      plan: mapping.plan,
      provisioningStatus: mapping.provisioningStatus,
      employee: agent ? agentDto(agent) : null,
      computer: computer ? computerDto(computer) : null,
      lastRun: runsByWorkspace.get(workspaceId) ?? null,
      nextRunAt: routinesByWorkspace.get(workspaceId)?.nextRunAt ?? null,
      openAlerts: alertsByWorkspace.get(workspaceId) ?? 0,
      usage: {
        records: usage?._count.id ?? 0,
        inputTokens: usage?._sum.inputTokens ?? 0,
        outputTokens: usage?._sum.outputTokens ?? 0,
        costMicros: (usage?._sum.costMicros ?? 0n).toString(),
        credential: credential
          ? {
              ...credential,
              monthlyLimitMicros: credential.monthlyLimitMicros.toString(),
              warningLimitMicros: credential.warningLimitMicros.toString(),
              currentUsageMicros: credential.currentUsageMicros.toString(),
              providerLimitMicros: credential.providerLimitMicros?.toString() ?? null,
            }
          : null,
      },
      sidekickCount: mappingSidekicks.filter(
        (sidekick) => !["canceled", "failed"].includes(sidekick.status.toLowerCase()),
      ).length,
      sidekicks: mappingSidekicks.map((sidekick) =>
        sidekickDto(sidekick, sidekickStats.get(sidekick.botId)),
      ),
    };
  });
}

async function workspaceDetail(prisma: PrismaClient, mapping: NonNullable<WorkspaceMapping>) {
  const [summary, routines, alerts, recentRuns, notifications, integrations, modelPolicy] =
    await Promise.all([
      fleetRow(prisma, mapping),
      prisma.routine.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId },
        orderBy: [{ active: "desc" }, { nextRunAt: "asc" }],
      }),
      prisma.brandwellAlert.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId },
        orderBy: [{ resolvedAt: "asc" }, { createdAt: "desc" }],
        take: 50,
      }),
      prisma.run.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId },
        orderBy: [{ createdAt: "desc" }],
        take: 50,
      }),
      prisma.brandwellClientNotification.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId },
        orderBy: [{ createdAt: "desc" }],
        take: 50,
      }),
      prisma.connection.findMany({
        where: { workspaceId: mapping.rakazoWorkspaceId, ownerType: "service" },
        orderBy: [{ status: "asc" }, { displayName: "asc" }],
      }),
      prisma.brandwellWorkspaceModelCredential.findUnique({
        where: { workspaceId: mapping.rakazoWorkspaceId },
      }),
    ]);
  const serializedModelPolicy = modelPolicy ? modelPolicyDto(modelPolicy) : null;
  const sidekickPolicies = summary.sidekicks
    .map((sidekick) => sidekick.modelPolicy)
    .filter((policy): policy is NonNullable<typeof policy> => Boolean(policy));
  const sidekickPolicyDrift = serializedModelPolicy
    ? sidekickPolicies.filter((policy) => !modelPoliciesMatch(serializedModelPolicy, policy)).length
    : sidekickPolicies.length;
  return {
    ...summary,
    routines,
    alerts,
    recentRuns,
    notifications,
    integrations: integrations.map(integrationDto),
    modelPolicy: serializedModelPolicy
      ? {
          ...serializedModelPolicy,
          managedCredentials: sidekickPolicies.length + 1,
          sidekickCredentials: sidekickPolicies.length,
          sidekickPolicyDrift,
        }
      : null,
  };
}

type SidekickWithResources = {
  botId: string | null;
};

type SidekickStats = {
  lastRun: Record<string, unknown> | null;
  nextRunAt: Date | null;
  openAlerts: number;
  usage: {
    records: number;
    inputTokens: number;
    outputTokens: number;
    costMicros: string;
  };
};

async function sidekickOperationalStats(
  prisma: PrismaClient,
  sidekicks: SidekickWithResources[],
): Promise<Map<string | null, SidekickStats>> {
  const botIds = sidekicks
    .map((sidekick) => sidekick.botId)
    .filter((botId): botId is string => Boolean(botId));
  const result = new Map<string | null, SidekickStats>();
  if (botIds.length === 0) return result;

  const [lastRuns, nextRoutines, alertCounts, usageGroups] = await Promise.all([
    prisma.run.findMany({
      where: { botId: { in: botIds } },
      orderBy: [{ botId: "asc" }, { createdAt: "desc" }],
      distinct: ["botId"],
    }),
    prisma.routine.findMany({
      where: { botId: { in: botIds }, active: true },
      orderBy: [{ botId: "asc" }, { nextRunAt: "asc" }],
      distinct: ["botId"],
    }),
    prisma.brandwellAlert.groupBy({
      by: ["botId"],
      where: {
        botId: { in: botIds },
        status: { notIn: ["RESOLVED", "IGNORED"] },
      },
      _count: { id: true },
    }),
    prisma.usageRecord.groupBy({
      by: ["botId"],
      where: { botId: { in: botIds } },
      _sum: { inputTokens: true, outputTokens: true, costMicros: true },
      _count: { id: true },
    }),
  ]);

  const runsByBot = new Map(lastRuns.map((run) => [run.botId, run]));
  const routinesByBot = new Map(nextRoutines.map((routine) => [routine.botId, routine]));
  const alertsByBot = new Map(alertCounts.map((row) => [row.botId, row._count.id]));
  const usageByBot = new Map(usageGroups.map((row) => [row.botId, row]));
  for (const botId of botIds) {
    const usage = usageByBot.get(botId);
    result.set(botId, {
      lastRun: (runsByBot.get(botId) as unknown as Record<string, unknown> | undefined) ?? null,
      nextRunAt: routinesByBot.get(botId)?.nextRunAt ?? null,
      openAlerts: alertsByBot.get(botId) ?? 0,
      usage: {
        records: usage?._count.id ?? 0,
        inputTokens: usage?._sum.inputTokens ?? 0,
        outputTokens: usage?._sum.outputTokens ?? 0,
        costMicros: (usage?._sum.costMicros ?? 0n).toString(),
      },
    });
  }
  return result;
}

async function usageDto(prisma: PrismaClient, workspaceId: string) {
  const [aggregate, credential] = await Promise.all([
    prisma.usageRecord.aggregate({
      where: { workspaceId },
      _sum: { inputTokens: true, outputTokens: true, costMicros: true },
      _count: { id: true },
    }),
    prisma.brandwellWorkspaceModelCredential.findUnique({
      where: { workspaceId },
      select: {
        status: true,
        monthlyLimitMicros: true,
        warningLimitMicros: true,
        currentUsageMicros: true,
        providerLimitMicros: true,
        providerUsageSyncedAt: true,
        providerUsageSyncError: true,
        preferredModel: true,
        disabledAt: true,
      },
    }),
  ]);
  return {
    records: aggregate._count.id,
    inputTokens: aggregate._sum.inputTokens ?? 0,
    outputTokens: aggregate._sum.outputTokens ?? 0,
    costMicros: (aggregate._sum.costMicros ?? 0n).toString(),
    credential: credential
      ? {
          ...credential,
          monthlyLimitMicros: credential.monthlyLimitMicros.toString(),
          warningLimitMicros: credential.warningLimitMicros.toString(),
          currentUsageMicros: credential.currentUsageMicros.toString(),
          providerLimitMicros: credential.providerLimitMicros?.toString() ?? null,
        }
      : null,
  };
}

function agentDto(agent: {
  id: string;
  name: string;
  title: string;
  description: string;
  instructions: string;
  additionalInstructions: string;
  managedStatus: string;
  computerId: string | null;
  updatedAt: Date;
}) {
  return {
    id: agent.id,
    name: agent.name,
    title: agent.title,
    description: agent.description,
    instructions: agent.instructions,
    additionalInstructions: agent.additionalInstructions,
    status: agent.managedStatus,
    computerId: agent.computerId,
    updatedAt: agent.updatedAt,
  };
}

function integrationDto(connection: {
  id: string;
  connectorId: string;
  provider: string;
  displayName: string;
  status: string;
  ownerType: string;
  updatedAt: Date;
}) {
  return {
    id: connection.id,
    connectorId: connection.connectorId,
    provider: connection.provider,
    displayName: connection.displayName,
    status: connection.status,
    ownerType: connection.ownerType,
    updatedAt: connection.updatedAt,
  };
}

function routineDto(routine: {
  id: string;
  botId: string;
  name: string;
  prompt: string;
  crons: string[];
  timezone: string;
  active: boolean;
  notify: boolean;
  lastRunAt: Date | null;
  nextRunAt: Date | null;
  updatedAt: Date;
}) {
  return {
    id: routine.id,
    botId: routine.botId,
    name: routine.name,
    prompt: routine.prompt,
    crons: routine.crons,
    timezone: routine.timezone,
    active: routine.active,
    notify: routine.notify,
    lastRunAt: routine.lastRunAt,
    nextRunAt: routine.nextRunAt,
    updatedAt: routine.updatedAt,
  };
}

function modelPolicyDto(policy: {
  id: string;
  provider: string;
  status: string;
  preferredModel: string;
  inheritsPlatformModelDefault?: boolean;
  computerModel: string | null;
  lightweightModel: string | null;
  reasoningModel: string | null;
  fallbackModels: unknown;
  modelCatalog: unknown;
  maxTokens: number | null;
  thinkingLevel: string | null;
  monthlyLimitMicros: bigint;
  dailyLimitMicros: bigint | null;
  warningLimitMicros: bigint;
  currentUsageMicros: bigint;
  providerLimitMicros: bigint | null;
  providerUsageSyncedAt: Date | null;
  providerUsageSyncError: string | null;
  disabledAt: Date | null;
  updatedAt: Date;
}) {
  return {
    id: policy.id,
    provider: policy.provider,
    status: policy.status,
    preferredModel: policy.preferredModel,
    inheritsPlatformModelDefault: policy.inheritsPlatformModelDefault ?? false,
    computerModel: policy.computerModel,
    lightweightModel: policy.lightweightModel,
    reasoningModel: policy.reasoningModel,
    fallbackModels: Array.isArray(policy.fallbackModels) ? policy.fallbackModels : [],
    modelCatalog: modelCatalogRecord(policy.modelCatalog),
    maxTokens: policy.maxTokens,
    thinkingLevel: policy.thinkingLevel,
    monthlyLimitMicros: policy.monthlyLimitMicros.toString(),
    dailyLimitMicros: policy.dailyLimitMicros?.toString() ?? null,
    warningLimitMicros: policy.warningLimitMicros.toString(),
    currentUsageMicros: policy.currentUsageMicros.toString(),
    providerLimitMicros: policy.providerLimitMicros?.toString() ?? null,
    providerUsageSyncedAt: policy.providerUsageSyncedAt,
    providerUsageSyncError: policy.providerUsageSyncError,
    disabledAt: policy.disabledAt,
    updatedAt: policy.updatedAt,
  };
}

function computerDto(computer: {
  id: string;
  state: string;
  scope: string;
  kind: string;
  controlHolder: string;
  controlActorType: string | null;
  controlActorName: string | null;
  controlStartedAt: Date | null;
  lastScreenshotAt: Date | null;
  lastComputerActivityAt: Date | null;
  lastComputerState: string | null;
  updatedAt: Date;
}) {
  return {
    id: computer.id,
    state: computer.state,
    scope: computer.scope,
    kind: computer.kind,
    controlHolder: computer.controlHolder,
    controlActorType: computer.controlActorType,
    controlActorName: computer.controlActorName,
    controlStartedAt: computer.controlStartedAt,
    lastScreenshotAt: computer.lastScreenshotAt,
    lastComputerActivityAt: computer.lastComputerActivityAt,
    lastComputerState: computer.lastComputerState,
    updatedAt: computer.updatedAt,
  };
}

function sidekickDto(
  sidekick: {
    id: string;
    brandwellSidekickId: string;
    email: string;
    name: string;
    roleTitle: string;
    status: string;
    userId: string | null;
    invitationId: string | null;
    skillBundleVersion: number;
    activatedAt: Date | null;
    pausedAt: Date | null;
    canceledAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
    bot: Parameters<typeof agentDto>[0] | null;
    computer: Parameters<typeof computerDto>[0] | null;
    modelCredential?: {
      id: string;
      provider: string;
      status: string;
      preferredModel: string;
      computerModel: string | null;
      lightweightModel: string | null;
      reasoningModel: string | null;
      fallbackModels: unknown;
      modelCatalog: unknown;
      maxTokens: number | null;
      thinkingLevel: string | null;
      monthlyLimitMicros: bigint;
      dailyLimitMicros: bigint | null;
      warningLimitMicros: bigint;
      currentUsageMicros: bigint;
      providerLimitMicros: bigint | null;
      providerUsageSyncedAt: Date | null;
      providerUsageSyncError: string | null;
      disabledAt: Date | null;
      updatedAt: Date;
    } | null;
  },
  stats?: SidekickStats,
) {
  return {
    id: sidekick.id,
    brandwellSidekickId: sidekick.brandwellSidekickId,
    email: sidekick.email,
    name: sidekick.name,
    roleTitle: sidekick.roleTitle,
    status: sidekick.status,
    access: sidekick.userId ? "member" : sidekick.invitationId ? "invited" : "pending",
    skillBundleVersion: sidekick.skillBundleVersion,
    activatedAt: sidekick.activatedAt,
    pausedAt: sidekick.pausedAt,
    canceledAt: sidekick.canceledAt,
    createdAt: sidekick.createdAt,
    updatedAt: sidekick.updatedAt,
    employee: sidekick.bot ? agentDto(sidekick.bot) : null,
    computer: sidekick.computer ? computerDto(sidekick.computer) : null,
    modelPolicy: sidekick.modelCredential ? modelPolicyDto(sidekick.modelCredential) : null,
    lastRun: stats?.lastRun ?? null,
    nextRunAt: stats?.nextRunAt ?? null,
    openAlerts: stats?.openAlerts ?? 0,
    usage: stats?.usage ?? {
      records: 0,
      inputTokens: 0,
      outputTokens: 0,
      costMicros: "0",
    },
  };
}

function modelPoliciesMatch(
  master: ReturnType<typeof modelPolicyDto>,
  sidekick: ReturnType<typeof modelPolicyDto>,
): boolean {
  return (
    master.provider === sidekick.provider &&
    master.preferredModel === sidekick.preferredModel &&
    master.computerModel === sidekick.computerModel &&
    master.lightweightModel === sidekick.lightweightModel &&
    master.reasoningModel === sidekick.reasoningModel &&
    JSON.stringify(master.fallbackModels) === JSON.stringify(sidekick.fallbackModels) &&
    JSON.stringify(master.modelCatalog) === JSON.stringify(sidekick.modelCatalog) &&
    master.maxTokens === sidekick.maxTokens &&
    master.thinkingLevel === sidekick.thinkingLevel &&
    master.monthlyLimitMicros === sidekick.monthlyLimitMicros &&
    master.dailyLimitMicros === sidekick.dailyLimitMicros &&
    master.warningLimitMicros === sidekick.warningLimitMicros
  );
}

function boundedLimit(value: string | undefined): number {
  const parsed = Number(value ?? 50);
  if (!Number.isFinite(parsed)) return 50;
  return Math.min(100, Math.max(1, Math.trunc(parsed)));
}

function workspaceCursorInput(
  value: string | undefined,
): { ok: true; value: { createdAt: Date; id: string } | null } | { ok: false; error: string } {
  if (value === undefined) return { ok: true, value: null };
  const encoded = value.trim();
  if (!encoded || encoded.length > 500 || !/^[A-Za-z0-9_-]+$/.test(encoded)) {
    return { ok: false, error: "cursor is invalid" };
  }
  try {
    const parsed = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error();
    const record = parsed as Record<string, unknown>;
    if (record.v !== 1 || typeof record.createdAt !== "string" || typeof record.id !== "string") {
      throw new Error();
    }
    const createdAt = new Date(record.createdAt);
    if (
      !Number.isFinite(createdAt.getTime()) ||
      createdAt.toISOString() !== record.createdAt ||
      !record.id ||
      record.id.length > 200
    ) {
      throw new Error();
    }
    return { ok: true, value: { createdAt, id: record.id } };
  } catch {
    return { ok: false, error: "cursor is invalid" };
  }
}

function encodeWorkspaceCursor(createdAt: Date, id: string): string {
  return Buffer.from(
    JSON.stringify({ v: 1, createdAt: createdAt.toISOString(), id }),
    "utf8",
  ).toString("base64url");
}

function managementIdempotencyKey(
  value: string | undefined,
): { ok: true; value: string } | { ok: false; error: string } {
  const key = String(value ?? "").trim();
  if (!/^[A-Za-z0-9._:-]{8,160}$/.test(key)) {
    return { ok: false, error: "A valid x-idempotency-key header is required" };
  }
  return { ok: true, value: key };
}

function employeeInstructionsInput(
  body: Record<string, unknown> | null,
): { ok: true; value: { instructions: string } } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  if (typeof body.instructions !== "string") {
    return { ok: false, error: "instructions is required" };
  }
  const instructions = body.instructions.trim();
  if (!instructions || instructions.length > 50_000) {
    return { ok: false, error: "instructions must contain 1 to 50000 characters" };
  }
  return { ok: true, value: { instructions } };
}

type RoutineSettingsInput = {
  name?: string;
  prompt?: string;
  crons?: string[];
  timezone?: string;
  active?: boolean;
  notify?: boolean;
};

function routineSettingsInput(
  body: Record<string, unknown> | null,
): { ok: true; value: RoutineSettingsInput } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const value: RoutineSettingsInput = {};
  if (body.name !== undefined) {
    if (typeof body.name !== "string" || !body.name.trim() || body.name.trim().length > 120) {
      return { ok: false, error: "name must contain 1 to 120 characters" };
    }
    value.name = body.name.trim();
  }
  if (body.prompt !== undefined) {
    if (
      typeof body.prompt !== "string" ||
      !body.prompt.trim() ||
      body.prompt.trim().length > 50_000
    ) {
      return { ok: false, error: "prompt must contain 1 to 50000 characters" };
    }
    value.prompt = body.prompt.trim();
  }
  if (body.crons !== undefined) {
    if (
      !Array.isArray(body.crons) ||
      body.crons.length < 1 ||
      body.crons.length > 8 ||
      body.crons.some((cron) => typeof cron !== "string" || !cron.trim() || cron.length > 120)
    ) {
      return { ok: false, error: "crons must contain 1 to 8 valid schedules" };
    }
    value.crons = body.crons.map((cron) => String(cron).trim());
  }
  if (body.timezone !== undefined) {
    if (typeof body.timezone !== "string" || !validTimezone(body.timezone.trim())) {
      return { ok: false, error: "timezone must be a valid IANA timezone" };
    }
    value.timezone = body.timezone.trim();
  }
  if (body.active !== undefined) {
    if (typeof body.active !== "boolean") return { ok: false, error: "active must be boolean" };
    value.active = body.active;
  }
  if (body.notify !== undefined) {
    if (typeof body.notify !== "boolean") return { ok: false, error: "notify must be boolean" };
    value.notify = body.notify;
  }
  if (Object.keys(value).length === 0) {
    return { ok: false, error: "At least one routine setting is required" };
  }
  return { ok: true, value };
}

function managedRoutineSchedule(
  crons: string[],
  timezone: string,
): { ok: true; nextRunAt: Date } | { ok: false; error: string } {
  if (hasMixedOneShotSchedule(crons) || isOneShotRoutineCrons(crons)) {
    return { ok: false, error: "BrandWell managed routines must use recurring schedules" };
  }
  try {
    const nextRunAt = nextCronDateAcrossStrict(crons, new Date(), timezone);
    if (!nextRunAt) return { ok: false, error: "Enter at least one recurring schedule" };
    return { ok: true, nextRunAt };
  } catch {
    return { ok: false, error: "Enter valid five-field cron schedules" };
  }
}

function validTimezone(value: string): boolean {
  if (!value || value.length > 100) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format();
    return true;
  } catch {
    return false;
  }
}

type ModelPolicyPatch = {
  inheritPlatformDefault?: boolean;
  preferredModel?: string;
  computerModel?: string | null;
  lightweightModel?: string | null;
  reasoningModel?: string | null;
  fallbackModels?: string[];
  maxTokens?: number | null;
  thinkingLevel?: string | null;
  monthlyLimitMicros?: bigint;
  dailyLimitMicros?: bigint | null;
  warningLimitMicros?: bigint;
};

type StoredModelPolicy = {
  inheritsPlatformModelDefault: boolean;
  preferredModel: string;
  computerModel: string | null;
  lightweightModel: string | null;
  reasoningModel: string | null;
  fallbackModels: unknown;
  modelCatalog: unknown;
  maxTokens: number | null;
  thinkingLevel: string | null;
  monthlyLimitMicros: bigint;
  dailyLimitMicros: bigint | null;
  warningLimitMicros: bigint;
};

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.length > 0);
}

function modelCatalogRecord(value: unknown): Record<string, ManagedModelCatalogEntry> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).filter(
      (entry): entry is [string, ManagedModelCatalogEntry] =>
        validOpenRouterModelId(entry[0]) &&
        Boolean(entry[1]) &&
        typeof entry[1] === "object" &&
        !Array.isArray(entry[1]),
    ),
  );
}

function resolvedModelPolicy(
  current: StoredModelPolicy,
  patch: ModelPolicyPatch,
  platformDefaultModel: string,
) {
  const inheritsPlatformModelDefault =
    patch.inheritPlatformDefault ??
    (patch.preferredModel !== undefined ? false : current.inheritsPlatformModelDefault);
  return {
    inheritsPlatformModelDefault,
    preferredModel: inheritsPlatformModelDefault
      ? platformDefaultModel
      : (patch.preferredModel ?? current.preferredModel),
    computerModel: patch.computerModel === undefined ? current.computerModel : patch.computerModel,
    lightweightModel:
      patch.lightweightModel === undefined ? current.lightweightModel : patch.lightweightModel,
    reasoningModel:
      patch.reasoningModel === undefined ? current.reasoningModel : patch.reasoningModel,
    fallbackModels: patch.fallbackModels ?? stringArray(current.fallbackModels),
    maxTokens: patch.maxTokens === undefined ? current.maxTokens : patch.maxTokens,
    thinkingLevel: patch.thinkingLevel === undefined ? current.thinkingLevel : patch.thinkingLevel,
    monthlyLimitMicros: patch.monthlyLimitMicros ?? current.monthlyLimitMicros,
    dailyLimitMicros:
      patch.dailyLimitMicros === undefined ? current.dailyLimitMicros : patch.dailyLimitMicros,
    warningLimitMicros: patch.warningLimitMicros ?? current.warningLimitMicros,
  };
}

function modelIdsFromPatch(patch: ModelPolicyPatch): string[] {
  const values = [
    patch.preferredModel,
    patch.computerModel,
    patch.lightweightModel,
    patch.reasoningModel,
    ...(patch.fallbackModels ?? []),
  ];
  return [...new Set(values.filter((value): value is string => typeof value === "string"))];
}

async function markModelPolicyProviderError(
  prisma: PrismaClient,
  workspaceId: string,
  message: string,
): Promise<void> {
  await prisma
    .$transaction([
      prisma.brandwellWorkspaceModelCredential.update({
        where: { workspaceId },
        data: { providerUsageSyncError: message },
      }),
      prisma.brandwellSidekickModelCredential.updateMany({
        where: { workspaceId },
        data: { providerUsageSyncError: message },
      }),
    ])
    .catch(() => undefined);
}

function modelPolicyInput(
  body: Record<string, unknown> | null,
): { ok: true; value: ModelPolicyPatch } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const value: ModelPolicyPatch = {};
  if (body.inheritPlatformDefault !== undefined) {
    if (typeof body.inheritPlatformDefault !== "boolean") {
      return { ok: false, error: "inheritPlatformDefault must be a boolean" };
    }
    value.inheritPlatformDefault = body.inheritPlatformDefault;
  }
  for (const field of [
    "preferredModel",
    "computerModel",
    "lightweightModel",
    "reasoningModel",
  ] as const) {
    if (body[field] === undefined) continue;
    if (field !== "preferredModel" && body[field] === null) {
      value[field] = null;
      continue;
    }
    if (typeof body[field] !== "string" || !validOpenRouterModelId(body[field].trim())) {
      return { ok: false, error: `${field} must be a valid OpenRouter vendor/model identifier` };
    }
    value[field] = body[field].trim();
  }
  if (body.fallbackModels !== undefined) {
    if (
      !Array.isArray(body.fallbackModels) ||
      body.fallbackModels.length > 10 ||
      body.fallbackModels.some(
        (model) => typeof model !== "string" || !validOpenRouterModelId(model.trim()),
      )
    ) {
      return {
        ok: false,
        error: "fallbackModels must contain up to 10 valid OpenRouter vendor/model identifiers",
      };
    }
    value.fallbackModels = [...new Set(body.fallbackModels.map((model) => String(model).trim()))];
  }
  if (body.maxTokens !== undefined) {
    if (body.maxTokens === null) value.maxTokens = null;
    else if (
      !Number.isInteger(body.maxTokens) ||
      Number(body.maxTokens) < 256 ||
      Number(body.maxTokens) > 1_000_000
    ) {
      return { ok: false, error: "maxTokens must be between 256 and 1000000" };
    } else value.maxTokens = Number(body.maxTokens);
  }
  if (body.thinkingLevel !== undefined) {
    if (body.thinkingLevel === null) value.thinkingLevel = null;
    else if (
      typeof body.thinkingLevel !== "string" ||
      !["none", "off", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"].includes(
        body.thinkingLevel,
      )
    ) {
      return { ok: false, error: "thinkingLevel is invalid" };
    } else value.thinkingLevel = body.thinkingLevel === "none" ? "off" : body.thinkingLevel;
  }
  for (const field of ["monthlyLimitMicros", "warningLimitMicros"] as const) {
    if (body[field] === undefined) continue;
    const parsed = nonnegativeBigInt(body[field]);
    if (parsed === null) return { ok: false, error: `${field} must be a nonnegative integer` };
    value[field] = parsed;
  }
  if (body.dailyLimitMicros !== undefined) {
    if (body.dailyLimitMicros === null) value.dailyLimitMicros = null;
    else {
      const parsed = nonnegativeBigInt(body.dailyLimitMicros);
      if (parsed === null) {
        return { ok: false, error: "dailyLimitMicros must be a nonnegative integer or null" };
      }
      value.dailyLimitMicros = parsed;
    }
  }
  if (Object.keys(value).length === 0) {
    return { ok: false, error: "At least one model policy setting is required" };
  }
  return { ok: true, value };
}

function platformModelPolicyInput(
  body: Record<string, unknown> | null,
): { ok: true; value: { modelId: string } } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  if (typeof body.modelId !== "string" || !validOpenRouterModelId(body.modelId.trim())) {
    return { ok: false, error: "modelId must be a valid OpenRouter vendor/model identifier" };
  }
  return { ok: true, value: { modelId: body.modelId.trim() } };
}

function nonnegativeBigInt(value: unknown): bigint | null {
  const text = typeof value === "bigint" ? value.toString() : String(value ?? "").trim();
  if (!/^\d{1,20}$/.test(text)) return null;
  try {
    return BigInt(text);
  } catch {
    return null;
  }
}

function validOpenRouterModelId(value: string): boolean {
  return /^[A-Za-z0-9~][A-Za-z0-9._~-]{0,99}\/[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$/.test(value);
}

function resolvedModelLimits(
  current: {
    monthlyLimitMicros: bigint;
    dailyLimitMicros: bigint | null;
    warningLimitMicros: bigint;
  },
  patch: ModelPolicyPatch,
): { ok: true } | { ok: false; error: string } {
  const monthly = patch.monthlyLimitMicros ?? current.monthlyLimitMicros;
  const daily =
    patch.dailyLimitMicros === undefined ? current.dailyLimitMicros : patch.dailyLimitMicros;
  const warning = patch.warningLimitMicros ?? current.warningLimitMicros;
  if (monthly <= 0n || monthly > 200_000_000n) {
    return {
      ok: false,
      error: "monthlyLimitMicros must cap managed OpenRouter usage at $200 or less",
    };
  }
  if (monthly > 0n && warning > monthly) {
    return { ok: false, error: "warningLimitMicros cannot exceed the monthly limit" };
  }
  if (monthly > 0n && daily !== null && daily > monthly) {
    return { ok: false, error: "dailyLimitMicros cannot exceed the monthly limit" };
  }
  return { ok: true };
}

function alertStatusInput(body: Record<string, unknown> | null):
  | {
      ok: true;
      value: {
        status:
          | "OPEN"
          | "ACKNOWLEDGED"
          | "WAITING_CLIENT"
          | "WAITING_BRANDWELL"
          | "RESOLVED"
          | "IGNORED";
      };
    }
  | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const status = String(body.status ?? "")
    .trim()
    .toUpperCase();
  if (
    ![
      "OPEN",
      "ACKNOWLEDGED",
      "WAITING_CLIENT",
      "WAITING_BRANDWELL",
      "RESOLVED",
      "IGNORED",
    ].includes(status)
  ) {
    return { ok: false, error: "status is invalid" };
  }
  return {
    ok: true,
    value: {
      status: status as
        | "OPEN"
        | "ACKNOWLEDGED"
        | "WAITING_CLIENT"
        | "WAITING_BRANDWELL"
        | "RESOLVED"
        | "IGNORED",
    },
  };
}

function clientNotificationInput(body: Record<string, unknown> | null):
  | {
      ok: true;
      value: {
        type: string;
        title: string;
        body: string;
        severity: string;
        requiresAction: boolean;
        targetBrandwellUserIds: string[] | null;
        actionType: string | null;
        actionTarget: string | null;
      };
    }
  | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  let targetBrandwellUserIds: string[] | null = null;
  if (body.targetBrandwellUserIds !== undefined) {
    const ids = body.targetBrandwellUserIds;
    if (
      !Array.isArray(ids) ||
      ids.length < 1 ||
      ids.length > 100 ||
      ids.some((id) => typeof id !== "string" || !/^[A-Za-z0-9._:-]{1,160}$/.test(id)) ||
      new Set(ids).size !== ids.length
    ) {
      return {
        ok: false,
        error: "targetBrandwellUserIds must contain 1 to 100 distinct user identifiers",
      };
    }
    targetBrandwellUserIds = ids as string[];
  }
  const title = typeof body.title === "string" ? body.title.trim() : "";
  const message = typeof body.body === "string" ? body.body.trim() : "";
  if (!title || title.length > 120) return { ok: false, error: "title is required" };
  if (!message || message.length > 1_000) return { ok: false, error: "body is required" };
  const type =
    typeof body.type === "string" && body.type.trim() ? body.type.trim().slice(0, 80) : "INFO";
  const severity =
    typeof body.severity === "string" && body.severity.trim()
      ? body.severity.trim().slice(0, 40)
      : "info";
  const actionType =
    typeof body.actionType === "string" && body.actionType.trim()
      ? body.actionType.trim().slice(0, 80)
      : null;
  const actionTarget =
    typeof body.actionTarget === "string" && body.actionTarget.trim()
      ? body.actionTarget.trim().slice(0, 500)
      : null;
  return {
    ok: true,
    value: {
      type,
      title,
      body: message,
      severity,
      requiresAction: body.requiresAction === true,
      targetBrandwellUserIds,
      actionType,
      actionTarget,
    },
  };
}

function clientNotificationResponse(row: ClientNotificationRecord, replayed: boolean) {
  return {
    id: row.id,
    type: row.type,
    title: row.title,
    severity: row.severity,
    requiresAction: row.requiresAction,
    createdAt: row.createdAt,
    replayed,
  };
}

async function supportComputerRequest(
  c: Context,
  deps: BrandwellManagementDeps,
  queryReason?: string,
): Promise<
  | { ok: true; value: BrandwellSupportRequest }
  | { ok: false; error: string; status: 400 | 404 | 409 | 503 }
> {
  if (!deps.computerSupport) {
    return { ok: false, error: "AIMEE computer support is not configured", status: 503 };
  }
  const mappingId = c.req.param("id");
  if (!mappingId) return { ok: false, error: "Workspace not found", status: 404 };
  const mapping = await findWorkspaceMapping(deps.prisma, mappingId);
  if (!mapping) return { ok: false, error: "Workspace not found", status: 404 };
  if (!["active", "trialing"].includes(mapping.subscriptionStatus)) {
    return { ok: false, error: "The AIMEE subscription is not active", status: 409 };
  }
  const actor = supportActor(c.req.header());
  if (!actor.ok) return { ok: false, error: actor.error, status: 400 };
  const body: Record<string, unknown> | null =
    c.req.method === "GET" ? null : await c.req.json<Record<string, unknown>>().catch(() => ({}));
  const rawReason = queryReason ?? (typeof body?.reason === "string" ? body.reason : "");
  const reason = rawReason.trim().slice(0, 500) || undefined;
  return {
    ok: true,
    value: {
      workspaceId: mapping.rakazoWorkspaceId,
      botId: mapping.primaryBotId,
      actor: actor.value,
      reason,
    },
  };
}

async function supportSidekickComputerRequest(
  c: Context,
  deps: BrandwellManagementDeps,
  queryReason?: string,
): Promise<
  | { ok: true; value: BrandwellSupportRequest }
  | { ok: false; error: string; status: 400 | 404 | 409 | 503 }
> {
  if (!deps.computerSupport) {
    return { ok: false, error: "AIMEE computer support is not configured", status: 503 };
  }
  const sidekickId = c.req.param("id");
  if (!sidekickId) return { ok: false, error: "Sidekick not found", status: 404 };
  const sidekick = await deps.prisma.brandwellSidekick.findFirst({
    where: {
      OR: [{ id: sidekickId }, { brandwellSidekickId: sidekickId }],
    },
    include: { aiWorkspace: true },
  });
  if (!sidekick?.botId || !sidekick.computerId) {
    return { ok: false, error: "Sidekick computer not found", status: 404 };
  }
  if (sidekick.status !== "active") {
    return { ok: false, error: "The AIMEE Sidekick is not active", status: 409 };
  }
  if (
    !sidekick.aiWorkspace ||
    !["active", "trialing"].includes(sidekick.aiWorkspace.subscriptionStatus)
  ) {
    return { ok: false, error: "The AIMEE subscription is not active", status: 409 };
  }
  const actor = supportActor(c.req.header());
  if (!actor.ok) return { ok: false, error: actor.error, status: 400 };
  const body: Record<string, unknown> | null =
    c.req.method === "GET" ? null : await c.req.json<Record<string, unknown>>().catch(() => ({}));
  const rawReason = queryReason ?? (typeof body?.reason === "string" ? body.reason : "");
  const reason = rawReason.trim().slice(0, 500) || undefined;
  return {
    ok: true,
    value: {
      workspaceId: sidekick.workspaceId,
      botId: sidekick.botId,
      actor: actor.value,
      reason,
    },
  };
}

export function supportActor(
  headers: Record<string, string>,
): { ok: true; value: BrandwellSupportActor } | { ok: false; error: string } {
  const reference = String(headers["x-brandwell-operator-ref"] ?? "").trim();
  const name = String(headers["x-brandwell-operator-name"] ?? "").trim();
  const email = String(headers["x-brandwell-operator-email"] ?? "")
    .trim()
    .toLowerCase();
  if (!/^[A-Za-z0-9._:@-]{1,160}$/.test(reference)) {
    return { ok: false, error: "A valid BrandWell operator reference is required" };
  }
  if (!name || name.length > 120) {
    return { ok: false, error: "A valid BrandWell operator name is required" };
  }
  if (email && (!email.includes("@") || email.length > 254)) {
    return { ok: false, error: "The BrandWell operator email is invalid" };
  }
  return { ok: true, value: { reference, name, ...(email ? { email } : {}) } };
}

function operatorAuditMetadata(actor: BrandwellSupportActor) {
  return {
    operatorReference: actor.reference,
    operatorName: actor.name,
    ...(actor.email ? { operatorEmail: actor.email } : {}),
  };
}

function supportComputerError(c: Context, error: unknown) {
  if (error instanceof BrandwellSupportComputerError) {
    return c.json({ error: error.message }, error.status);
  }
  console.error("BrandWell support computer operation", error);
  return c.json({ error: "AIMEE could not complete the computer support request" }, 503);
}

function provisioningInput(
  body: Record<string, unknown> | null,
): { ok: true; value: BrandwellProvisioningInput } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const required = [
    "brandwellCustomerId",
    "primaryBrandwellUserId",
    "companyName",
    "primaryContactName",
    "primaryContactEmail",
    "timezone",
  ] as const;
  for (const field of required) {
    if (typeof body[field] !== "string" || !body[field].trim()) {
      return { ok: false, error: `${field} is required` };
    }
  }
  return {
    ok: true,
    value: {
      brandwellCustomerId: String(body.brandwellCustomerId),
      primaryBrandwellUserId: String(body.primaryBrandwellUserId),
      companyName: String(body.companyName),
      primaryContactName: String(body.primaryContactName),
      primaryContactEmail: String(body.primaryContactEmail),
      plan: typeof body.plan === "string" && body.plan.trim() ? body.plan : "aimee",
      timezone: String(body.timezone),
    },
  };
}

function desiredStateInput(
  body: Record<string, unknown> | null,
): { ok: true; value: BrandwellWorkspaceDesiredStateInput } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const revision = nonnegativeBigInt(body.revision);
  if (revision === null || revision < 1n) {
    return { ok: false, error: "revision must be a positive integer" };
  }
  const agencyId = compactIdentifier(body.agencyId);
  const clientId = compactIdentifier(body.clientId);
  const primaryBrandwellUserId = compactIdentifier(body.primaryBrandwellUserId);
  const contractId = body.contractId === null ? null : compactIdentifier(body.contractId);
  if (
    !agencyId ||
    !clientId ||
    !primaryBrandwellUserId ||
    (body.contractId !== undefined && body.contractId !== null && !contractId)
  ) {
    return {
      ok: false,
      error: "agencyId, clientId, contractId, and primaryBrandwellUserId must be valid identifiers",
    };
  }
  const status = String(body.status ?? "").trim();
  if (!["trialing", "active", "past_due", "paused", "canceling", "canceled"].includes(status)) {
    return { ok: false, error: "status is invalid" };
  }
  const plan = compactIdentifier(body.plan);
  const masterSeats = Number(body.masterSeats);
  const sidekickSeats = Number(body.sidekickSeats);
  const skillBundleVersion = Number(body.skillBundleVersion);
  if (
    !plan ||
    masterSeats !== 1 ||
    !Number.isSafeInteger(sidekickSeats) ||
    sidekickSeats < 0 ||
    sidekickSeats > 10_000 ||
    !Number.isSafeInteger(skillBundleVersion) ||
    skillBundleVersion < 1
  ) {
    return { ok: false, error: "The desired AIMEE entitlement is invalid" };
  }
  return {
    ok: true,
    value: {
      revision,
      agencyId,
      clientId,
      contractId,
      primaryBrandwellUserId,
      status: status as BrandwellWorkspaceDesiredStateInput["status"],
      plan,
      masterSeats: 1,
      sidekickSeats,
      skillBundleVersion,
    },
  };
}

function sidekickProvisioningInput(
  body: Record<string, unknown> | null,
): { ok: true; value: BrandwellSidekickProvisioningInput } | { ok: false; error: string } {
  if (!body) return { ok: false, error: "A JSON request body is required" };
  const brandwellSidekickId = compactIdentifier(body.brandwellSidekickId);
  const brandwellUserId = compactIdentifier(body.brandwellUserId);
  const email = typeof body.email === "string" ? body.email.trim().toLowerCase() : "";
  const name = typeof body.name === "string" ? body.name.replace(/\s+/g, " ").trim() : "";
  const roleTitle =
    typeof body.roleTitle === "string" ? body.roleTitle.replace(/\s+/g, " ").trim() : "";
  const timezone = typeof body.timezone === "string" ? body.timezone.trim() : "";
  if (!brandwellSidekickId) return { ok: false, error: "brandwellSidekickId is invalid" };
  if (!brandwellUserId) return { ok: false, error: "brandwellUserId is invalid" };
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 254) {
    return { ok: false, error: "email is invalid" };
  }
  if (!name || name.length > 160) return { ok: false, error: "name is required" };
  if (!roleTitle || roleTitle.length > 160) return { ok: false, error: "roleTitle is required" };
  if (!validTimezone(timezone))
    return { ok: false, error: "timezone must be a valid IANA timezone" };
  return {
    ok: true,
    value: { brandwellSidekickId, brandwellUserId, email, name, roleTitle, timezone },
  };
}

function compactIdentifier(value: unknown): string | null {
  const text = typeof value === "string" ? value.trim() : String(value ?? "").trim();
  return /^[A-Za-z0-9._:-]{1,200}$/.test(text) ? text : null;
}

function brandwellSidekickError(c: Context, error: unknown, fallback: string) {
  if (error instanceof BrandwellSidekickError) {
    const status = [400, 404, 409, 503].includes(error.statusCode) ? error.statusCode : 500;
    return c.json(
      { error: error.message, code: error.code },
      status as 400 | 404 | 409 | 500 | 503,
    );
  }
  console.error(fallback, error);
  return c.json({ error: fallback }, 500);
}
