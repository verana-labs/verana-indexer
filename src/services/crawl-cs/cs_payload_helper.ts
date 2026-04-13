import { parseCredentialSchemaEvent } from "./cs_event_mapper";

export function enrichSchemaMessageWithEvent(
  schemaMessage: any,
  txResponse: any
) {
  if (!txResponse) return schemaMessage;

  const eventData = parseCredentialSchemaEvent(txResponse);
  if (!eventData) return schemaMessage;

  const content = schemaMessage.content ?? {};
  const updatedContent = { ...content };

  const ensure = (key: string, value: any) => {
    if (
      updatedContent[key] === undefined ||
      updatedContent[key] === null ||
      updatedContent[key] === 0
    ) {
      updatedContent[key] = value;
    }
  };

  ensure(
    "issuer_grantor_validation_validity_period",
    eventData.issuer_grantor_validation_validity_period
  );

  ensure(
    "verifier_grantor_validation_validity_period",
    eventData.verifier_grantor_validation_validity_period
  );

  ensure(
    "issuer_validation_validity_period",
    eventData.issuer_validation_validity_period
  );

  ensure(
    "verifier_validation_validity_period",
    eventData.verifier_validation_validity_period
  );

  ensure(
    "holder_validation_validity_period",
    eventData.holder_validation_validity_period
  );
  ensure(
    "archived",
    eventData.archived ? eventData.archived.toISOString() : null
  );
  ensure(
    "verifier_onboarding_mode",
    eventData.verifier_onboarding_mode
  );
  ensure(
    "issuer_onboarding_mode",
    eventData.issuer_onboarding_mode
  );

  const id = schemaMessage.id ?? eventData.id;
  ensure("tr_id", eventData.tr_id);

  return {
    ...schemaMessage,
    id,
    content: updatedContent,
  };
}
