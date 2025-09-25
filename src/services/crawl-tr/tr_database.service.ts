/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import ModuleParams from "../../models/modules_params";
import { TrustRegistry } from "../../models/trust_registry";

@Service({
    name: SERVICE.V1.TrustRegistryDatabaseService.key,
    version: 1
})
export default class TrustRegistryDatabaseService extends BaseService {
    public constructor(public broker: ServiceBroker) {
        super(broker);
    }
    @Action()
    public async getTrustRegistry(ctx: Context<{
        tr_id: number;
        active_gf_only?: string | boolean;
        preferred_language?: string;
    }>) {
        try {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const { tr_id, preferred_language } = ctx.params;
            const activeGfOnly =
                String(ctx.params.active_gf_only).toLowerCase() === "true";

            const registry = await TrustRegistry.query()
                .findById(tr_id)
                .withGraphFetched("governanceFrameworkVersions.documents");

            if (!registry) {
                return ApiResponder.error(ctx, `TrustRegistry with id ${tr_id} not found`, 404);
            }

            const plain = registry.toJSON();
            let versions = plain.governanceFrameworkVersions ?? [];

            if (activeGfOnly) {
                versions = versions
                    .sort(
                        (a, b) =>
                            new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
                    )
                    .slice(0, 1);
            }

            if (preferred_language) {
                for (const v of versions) {
                    v.documents =
                        v.documents?.filter((d) => d.language === preferred_language) ?? [];
                }
            }

            delete plain.governanceFrameworkVersions;

            return ApiResponder.success(ctx, { ...plain, versions }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action()
    public async listTrustRegistries(ctx: Context<{
        controller?: string;
        modified_after?: string;
        active_gf_only?: string | boolean;
        preferred_language?: string;
        response_max_size?: number;
    }>) {
        try {
            const {
                controller,
                modified_after: modifiedAfter,
                preferred_language: preferredLanguage,
                response_max_size: responseMaxSizeRaw
            } = ctx.params;

            const activeGfOnly =
                String(ctx.params.active_gf_only).toLowerCase() === "true";

            const responseMaxSize =
                !responseMaxSizeRaw ? 64 : Math.min(Math.max(responseMaxSizeRaw, 1), 1024);

            if (responseMaxSizeRaw && (responseMaxSizeRaw < 1 || responseMaxSizeRaw > 1024)) {
                return ApiResponder.error(ctx, "response_max_size must be between 1 and 1024", 400);
            }

            let query = TrustRegistry.query().withGraphFetched("governanceFrameworkVersions.documents");

            if (controller) {
                query = query.where("controller", controller);
            }

            if (modifiedAfter) {
                query = query.where("modified", ">", modifiedAfter).orderBy("modified", "desc");
            } else {
                query = query.orderBy("modified", "asc");
            }

            const registries = await query.limit(responseMaxSize);

            const result = registries.map((tr) => {
                const plain = tr.toJSON();
                let versions = plain.governanceFrameworkVersions ?? [];

                if (activeGfOnly) {
                    versions = versions
                        .sort((a, b) => new Date(b.active_since).getTime() - new Date(a.active_since).getTime())
                        .slice(0, 1);
                }

                if (preferredLanguage) {
                    for (const v of versions) {
                        v.documents =
                            v.documents?.filter((d) => d.language === preferredLanguage) ?? [];
                    }
                }

                delete plain.governanceFrameworkVersions;

                return { ...plain, versions };
            });

            return ApiResponder.success(ctx, result, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action()
    public async getParams(ctx: Context) {
        try {
            const module = await ModuleParams.query().findOne({ module: "trustregistry" });

            if (!module || !module.params) {
                return ApiResponder.error(ctx, "Module parameters not found: trustregistry", 404);
            }

            const parsedParams =
                typeof module.params === "string"
                    ? JSON.parse(module.params)
                    : module.params;

            return ApiResponder.success(ctx, parsedParams.params || {}, 200);
        } catch (err: any) {
            this.logger.error("Error fetching trustregistry params", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }
}