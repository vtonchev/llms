import {
  FastifyInstance,
  FastifyPluginAsync,
  FastifyRequest,
  FastifyReply,
} from "fastify";
import { RegisterProviderRequest, LLMProvider } from "@/types/llm";
import { sendUnifiedRequest } from "@/utils/request";
import { requestQueue } from "@/utils/requestQueue";
import { createApiError } from "./middleware";
import { version } from "../../package.json";

/**
 * Main function to handle transformer endpoints.
 * Coordinates the entire request processing flow: validating the provider,
 * processing the request transformer, sending the request, processing the
 * response transformer, and formatting the response.
 */
async function handleTransformerEndpoint(
  req: FastifyRequest,
  reply: FastifyReply,
  fastify: FastifyInstance,
  transformer: any
) {
  const body = req.body as any;
  const providerName = req.provider!;
  const provider = fastify._server!.providerService.getProvider(providerName);

  // 验证提供者是否存在
  if (!provider) {
    throw createApiError(
      `Provider '${providerName}' not found`,
      404,
      "provider_not_found"
    );
  }

  // 处理请求转换器链

  const { requestBody, config, bypass } = await processRequestTransformers(
    body,
    provider,
    transformer,
    req.headers,
    {
      req,
    }
  );

  req.log.info({ requestBody, config: config, provider, bypass, transformer }, '[handleTransformerEndpoint] Before sendRequestToProvider');

  // Queue and send request to LLM provider (serialized per provider)
  const queueLength = requestQueue.getQueueLength(providerName);
  if (queueLength > 0) {
    req.log.info({ provider: providerName, queueLength }, '[handleTransformerEndpoint] Request queued');
  }

  const response = await requestQueue.enqueue(providerName, () =>
    sendRequestToProvider(
      requestBody,
      config,
      provider,
      fastify,
      bypass,
      transformer,
      {
        req,
      }
    )
  );

  // Log response with stream content (tee the stream to preserve it)
  let logResponse = response;
  if (response.body) {
    const [logStream, processStream] = response.body.tee();
    // Read and log the stream content in background
    (async () => {
      const reader = logStream.getReader();
      const chunks: Uint8Array[] = [];
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          chunks.push(value);
        }
        const text = new TextDecoder().decode(Buffer.concat(chunks.map(c => Buffer.from(c))));
        req.log.info({ streamBody: text.slice(0, 5000) }, '[handleTransformerEndpoint] Response stream content');
      } catch (e) {
        req.log.error({ error: e }, '[handleTransformerEndpoint] Error reading stream');
      }
    })();
    // Create new response with the process stream
    logResponse = new Response(processStream, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers
    });
  }

  req.log.info({
    response: {
      status: response.status,
      statusText: response.statusText,
      headers: Object.fromEntries(response.headers.entries()),
      ok: response.ok,
      type: response.type,
      url: response.url
    }
  }, '[handleTransformerEndpoint] After sendRequestToProvider');

  // 处理响应转换器链
  const finalResponse = await processResponseTransformers(
    requestBody,
    logResponse,
    provider,
    transformer,
    bypass,
    {
      req,
    }
  );

  // 格式化并返回响应
  return formatResponse(finalResponse, reply, body);
}

/**
 * 处理请求转换器链
 * 依次执行transformRequestOut、provider transformers、model-specific transformers
 * 返回处理后的请求体、配置和是否跳过转换器的标志
 */
async function processRequestTransformers(
  body: any,
  provider: any,
  transformer: any,
  headers: any,
  context: any
) {
  let requestBody = body;
  let config = {};
  let bypass = false;

  // 检查是否应该跳过转换器（透传参数）
  bypass = shouldBypassTransformers(provider, transformer, body);

  if (bypass) {
    if (headers instanceof Headers) {
      headers.delete("content-length");
    } else {
      delete headers["content-length"];
    }
    config.headers = headers;
  }

  // 执行transformer的transformRequestOut方法
  if (!bypass && typeof transformer.transformRequestOut === "function") {
    const transformOut = await transformer.transformRequestOut(requestBody);
    if (transformOut.body) {
      requestBody = transformOut.body;
      config = transformOut.config || {};
    } else {
      requestBody = transformOut;
    }
  }

  // 执行provider级别的转换器
  if (!bypass && provider.transformer?.use?.length) {
    for (const providerTransformer of provider.transformer.use) {
      if (
        !providerTransformer ||
        typeof providerTransformer.transformRequestIn !== "function"
      ) {
        continue;
      }
      const transformIn = await providerTransformer.transformRequestIn(
        requestBody,
        provider,
        context
      );
      if (transformIn.body) {
        requestBody = transformIn.body;
        config = { ...config, ...transformIn.config };
      } else {
        requestBody = transformIn;
      }
    }
  }

  // 执行模型特定的转换器
  if (!bypass && provider.transformer?.[body.model]?.use?.length) {
    for (const modelTransformer of provider.transformer[body.model].use) {
      if (
        !modelTransformer ||
        typeof modelTransformer.transformRequestIn !== "function"
      ) {
        continue;
      }
      requestBody = await modelTransformer.transformRequestIn(
        requestBody,
        provider,
        context
      );
    }
  }

  return { requestBody, config, bypass };
}

/**
 * 判断是否应该跳过转换器（透传参数）
 * 当provider只使用一个transformer且该transformer与当前transformer相同时，跳过其他转换器
 */
function shouldBypassTransformers(
  provider: any,
  transformer: any,
  body: any
): boolean {
  return (
    provider.transformer?.use?.length === 1 &&
    provider.transformer.use[0].name === transformer.name &&
    (!provider.transformer?.[body.model]?.use.length ||
      (provider.transformer?.[body.model]?.use.length === 1 &&
        provider.transformer?.[body.model]?.use[0].name === transformer.name))
  );
}

/**
 * 发送请求到LLM提供者
 * 处理认证、构建请求配置、发送请求并处理错误
 */
async function sendRequestToProvider(
  requestBody: any,
  config: any,
  provider: any,
  fastify: FastifyInstance,
  bypass: boolean,
  transformer: any,
  context: any
) {
  const url = config.url || new URL(provider.baseUrl);

  // 在透传参数下处理认证
  if (bypass && typeof transformer.auth === "function") {
    const auth = await transformer.auth(requestBody, provider);
    if (auth.body) {
      requestBody = auth.body;
      let headers = config.headers || {};
      if (auth.config?.headers) {
        headers = {
          ...headers,
          ...auth.config.headers,
        };
        delete headers.host;
        delete auth.config.headers;
      }
      config = {
        ...config,
        ...auth.config,
        headers,
      };
    } else {
      requestBody = auth;
    }
  }

  // 发送HTTP请求
  // 准备headers
  const requestHeaders: Record<string, string> = {
    Authorization: `Bearer ${provider.apiKey}`,
    ...(config?.headers || {}),
  };

  for (const key in requestHeaders) {
    if (requestHeaders[key] === "undefined") {
      delete requestHeaders[key];
    } else if (
      ["authorization", "Authorization"].includes(key) &&
      requestHeaders[key]?.includes("undefined")
    ) {
      delete requestHeaders[key];
    }
  }

  // Helper function to make the actual request
  const makeRequest = async () => {
    return sendUnifiedRequest(
      url,
      requestBody,
      {
        httpsProxy: fastify._server!.configService.getHttpsProxy(),
        ...config,
        headers: JSON.parse(JSON.stringify(requestHeaders)),
      },
      fastify.log,
      context
    );
  };

  let response = await makeRequest();

  // Handle 429 rate limit with 1 retry
  if (response.status === 429) {
    const errorText = await response.text();
    fastify.log.warn(
      `[rate_limit] Got 429 from provider(${provider.name}), attempting retry...`
    );

    // Try to parse retryDelay from the response
    let retryDelayMs = 1000; // Default 1 second
    try {
      const errorBody = JSON.parse(errorText);
      const retryInfo = errorBody?.error?.details?.find(
        (d: any) => d["@type"]?.includes("RetryInfo")
      );
      if (retryInfo?.retryDelay) {
        // Parse "1.085714732s" format
        const delayStr = retryInfo.retryDelay;
        const seconds = parseFloat(delayStr.replace("s", ""));
        if (!isNaN(seconds)) {
          retryDelayMs = Math.ceil(seconds * 1000);
        }
      }
    } catch (e) {
      // Use default delay if parsing fails
    }

    fastify.log.info(
      `[rate_limit] Waiting ${retryDelayMs}ms before retry...`
    );
    await new Promise((resolve) => setTimeout(resolve, retryDelayMs));

    // Retry once
    response = await makeRequest();

    // If still failing after retry, throw the error
    if (!response.ok) {
      const retryErrorText = await response.text();
      fastify.log.error(
        `[provider_response_error] Error from provider after retry(${provider.name},${requestBody.model}: ${response.status}): ${retryErrorText}`
      );
      throw createApiError(
        `Error from provider(${provider.name},${requestBody.model}: ${response.status}): ${retryErrorText}`,
        response.status,
        "provider_response_error"
      );
    }

    fastify.log.info(`[rate_limit] Retry successful for provider(${provider.name})`);
  } else if (!response.ok) {
    // Handle other errors (non-429)
    const errorText = await response.text();
    fastify.log.error(
      `[provider_response_error] Error from provider(${provider.name},${requestBody.model}: ${response.status}): ${errorText}`
    );
    throw createApiError(
      `Error from provider(${provider.name},${requestBody.model}: ${response.status}): ${errorText}`,
      response.status,
      "provider_response_error"
    );
  }

  return response;
}

/**
 * 处理响应转换器链
 * 依次执行provider transformers、model-specific transformers、transformer的transformResponseIn
 */
async function processResponseTransformers(
  requestBody: any,
  response: any,
  provider: any,
  transformer: any,
  bypass: boolean,
  context: any
) {
  let finalResponse = response;

  // 执行provider级别的响应转换器
  context.req?.log?.info?.({ bypass, hasTransformerUse: !!provider.transformer?.use?.length, transformerUseLength: provider.transformer?.use?.length }, '[processResponseTransformers] Provider check');
  if (!bypass && provider.transformer?.use?.length) {
    for (const providerTransformer of Array.from(
      provider.transformer.use
    ).reverse()) {
      if (
        !providerTransformer ||
        typeof providerTransformer.transformResponseOut !== "function"
      ) {
        continue;
      }
      finalResponse = await providerTransformer.transformResponseOut(
        finalResponse,
        context
      );
      // Log headers after transformer to debug
      const headersObj: Record<string, string> = {};
      finalResponse.headers?.forEach?.((v: string, k: string) => { headersObj[k] = v; });
      context.req?.log?.info?.({ headers: headersObj }, '[processResponseTransformers] After provider transformer');
    }
  }

  // 执行模型特定的响应转换器
  if (!bypass && provider.transformer?.[requestBody.model]?.use?.length) {
    for (const modelTransformer of Array.from(
      provider.transformer[requestBody.model].use
    ).reverse()) {
      if (
        !modelTransformer ||
        typeof modelTransformer.transformResponseOut !== "function"
      ) {
        continue;
      }
      finalResponse = await modelTransformer.transformResponseOut(
        finalResponse,
        context
      );
    }
  }

  // 执行transformer的transformResponseIn方法
  // Skip if response has X-Skip-Response-Transform header (e.g., Antigravity already outputs Anthropic format)
  const skipHeader = finalResponse.headers?.get?.('X-Skip-Response-Transform');
  const skipResponseTransform = skipHeader === 'true';
  console.log('[routes] Skip header check:', { skipHeader, skipResponseTransform, hasHeaders: !!finalResponse.headers });
  if (!bypass && !skipResponseTransform && transformer.transformResponseIn) {
    finalResponse = await transformer.transformResponseIn(
      finalResponse,
      context
    );
  }

  return finalResponse;
}

/**
 * 格式化并返回响应
 * 处理HTTP状态码、流式响应和普通响应的格式化
 */
function formatResponse(response: any, reply: FastifyReply, body: any) {
  // 设置HTTP状态码
  if (!response.ok) {
    reply.code(response.status);
  }

  // 处理流式响应 - check both body.stream and response Content-Type
  const contentType = response.headers?.get?.("Content-Type") || "";
  const isStream = body.stream === true || contentType.includes("text/event-stream") || contentType.includes("stream");

  reply.log.info({ isStream, bodyStream: body.stream, contentType }, '[formatResponse] Stream detection');

  if (isStream) {
    reply.header("Content-Type", "text/event-stream");
    reply.header("Cache-Control", "no-cache");
    reply.header("Connection", "keep-alive");
    return reply.send(response.body);
  } else {
    // 处理普通JSON响应
    reply.log.info('[formatResponse] Calling response.json() for non-streaming response');
    return response.json();
  }
}

export const registerApiRoutes: FastifyPluginAsync = async (
  fastify: FastifyInstance
) => {
  // Health and info endpoints
  fastify.get("/", async () => {
    return { message: "LLMs API", version };
  });

  fastify.get("/health", async () => {
    return { status: "ok", timestamp: new Date().toISOString() };
  });

  const transformersWithEndpoint =
    fastify._server!.transformerService.getTransformersWithEndpoint();

  for (const { transformer } of transformersWithEndpoint) {
    if (transformer.endPoint) {
      fastify.post(
        transformer.endPoint,
        async (req: FastifyRequest, reply: FastifyReply) => {
          return handleTransformerEndpoint(req, reply, fastify, transformer);
        }
      );
    }
  }

  fastify.post(
    "/providers",
    {
      schema: {
        body: {
          type: "object",
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            type: { type: "string", enum: ["openai", "anthropic"] },
            baseUrl: { type: "string" },
            apiKey: { type: "string" },
            models: { type: "array", items: { type: "string" } },
          },
          required: ["id", "name", "type", "baseUrl", "apiKey", "models"],
        },
      },
    },
    async (
      request: FastifyRequest<{ Body: RegisterProviderRequest }>,
      reply: FastifyReply
    ) => {
      // Validation
      const { name, baseUrl, apiKey, models } = request.body;

      if (!name?.trim()) {
        throw createApiError(
          "Provider name is required",
          400,
          "invalid_request"
        );
      }

      if (!baseUrl || !isValidUrl(baseUrl)) {
        throw createApiError(
          "Valid base URL is required",
          400,
          "invalid_request"
        );
      }

      if (!apiKey?.trim()) {
        throw createApiError("API key is required", 400, "invalid_request");
      }

      if (!models || !Array.isArray(models) || models.length === 0) {
        throw createApiError(
          "At least one model is required",
          400,
          "invalid_request"
        );
      }

      // Check if provider already exists
      if (fastify._server!.providerService.getProvider(request.body.name)) {
        throw createApiError(
          `Provider with name '${request.body.name}' already exists`,
          400,
          "provider_exists"
        );
      }

      return fastify._server!.providerService.registerProvider(request.body);
    }
  );

  fastify.get("/providers", async () => {
    return fastify._server!.providerService.getProviders();
  });

  fastify.get(
    "/providers/:id",
    {
      schema: {
        params: {
          type: "object",
          properties: { id: { type: "string" } },
          required: ["id"],
        },
      },
    },
    async (request: FastifyRequest<{ Params: { id: string } }>) => {
      const provider = fastify._server!.providerService.getProvider(
        request.params.id
      );
      if (!provider) {
        throw createApiError("Provider not found", 404, "provider_not_found");
      }
      return provider;
    }
  );

  fastify.put(
    "/providers/:id",
    {
      schema: {
        params: {
          type: "object",
          properties: { id: { type: "string" } },
          required: ["id"],
        },
        body: {
          type: "object",
          properties: {
            name: { type: "string" },
            type: { type: "string", enum: ["openai", "anthropic"] },
            baseUrl: { type: "string" },
            apiKey: { type: "string" },
            models: { type: "array", items: { type: "string" } },
            enabled: { type: "boolean" },
          },
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: Partial<LLMProvider>;
      }>,
      reply
    ) => {
      const provider = fastify._server!.providerService.updateProvider(
        request.params.id,
        request.body
      );
      if (!provider) {
        throw createApiError("Provider not found", 404, "provider_not_found");
      }
      return provider;
    }
  );

  fastify.delete(
    "/providers/:id",
    {
      schema: {
        params: {
          type: "object",
          properties: { id: { type: "string" } },
          required: ["id"],
        },
      },
    },
    async (request: FastifyRequest<{ Params: { id: string } }>) => {
      const success = fastify._server!.providerService.deleteProvider(
        request.params.id
      );
      if (!success) {
        throw createApiError("Provider not found", 404, "provider_not_found");
      }
      return { message: "Provider deleted successfully" };
    }
  );

  fastify.patch(
    "/providers/:id/toggle",
    {
      schema: {
        params: {
          type: "object",
          properties: { id: { type: "string" } },
          required: ["id"],
        },
        body: {
          type: "object",
          properties: { enabled: { type: "boolean" } },
          required: ["enabled"],
        },
      },
    },
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: { enabled: boolean };
      }>,
      reply
    ) => {
      const success = fastify._server!.providerService.toggleProvider(
        request.params.id,
        request.body.enabled
      );
      if (!success) {
        throw createApiError("Provider not found", 404, "provider_not_found");
      }
      return {
        message: `Provider ${request.body.enabled ? "enabled" : "disabled"
          } successfully`,
      };
    }
  );
};

// Helper function
function isValidUrl(url: string): boolean {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}
