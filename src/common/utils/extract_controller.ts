export function extractController(
  message: Record<string, any>,
  fallback?: string
): string | undefined {
  if (!message || typeof message !== 'object') {
    return fallback;
  }

  const controller = 
    message.controller || 
    message.creator || 
    message.authority ||
    message.sender ||
    message.account ||
    message.grantee ||
    message.created_by;

  if (controller && typeof controller === 'string' && controller.trim()) {
    return controller.trim();
  }

  return fallback;
}

export function requireController(
  message: Record<string, any>,
  fieldName: string = 'Record'
): string {
  const controller = extractController(message);
  
  if (!controller) {
    throw new Error(
      `${fieldName}: Missing required controller/creator field. ` +
      `Message keys: ${Object.keys(message).join(', ')}`
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
    controller,
  };
}
