/** Extracts the VPR v4 participant address from decoded module message content (corporation / operator). */
export function extractController(
  message: Record<string, any>,
  fallback?: string
): string | undefined {
  if (!message || typeof message !== "object") {
    return fallback;
  }

  const controller =
    message.corporation ??
    message.operator ??
    message.creator ??
    message.sender;

  if (controller && typeof controller === "string" && controller.trim()) {
    return controller.trim();
  }

  return fallback;
}

export function requireController(
  message: Record<string, any>,
  fieldName: string = "Record"
): string {
  const controller = extractController(message);

  if (!controller) {
    throw new Error(
      `${fieldName}: Missing required corporation/operator field. ` +
        `Message keys: ${Object.keys(message).join(", ")}`
    );
  }

  return controller;
}

export function normalizeController(
  message: Record<string, any>,
  fallback?: string
): Record<string, any> {
  const controller = extractController(message, fallback);

  return {
    ...message,
    corporation: controller,
  };
}
