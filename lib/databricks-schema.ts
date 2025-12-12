import { z } from "zod";

export const databricksSchema = z.object({
  apiUrl: z.string()
    .url("Invalid URL")
    .includes("databricks", { message: "Must be a Databricks URL" }),
  accessToken: z.string()
    .min(10, "Token must be at least 10 characters"),
  catalogName: z.string(),
  schemaName: z.string(),
  warehouseId: z.string()
});

export type DatabricksFormValues = z.infer<typeof databricksSchema>;