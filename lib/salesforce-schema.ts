import { z } from "zod";

export const salesforceSchema = z.object({
  username: z.string().email(),
  password: z.string().min(6),
  securityToken: z.string().min(6),
  loginUrl: z.string().url()
});

export type SalesforceFormValues = z.infer<typeof salesforceSchema>;