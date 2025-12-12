"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { salesforceSchema, SalesforceFormValues } from "@/lib/salesforce-schema";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useRouter } from 'next/navigation';
import { toast } from 'sonner';
import { useState } from 'react';
import { useLocalStorage } from "@/hooks/use-local-storage";

export default function SalesforcePage() {
  const router = useRouter();
  const [isTesting, setIsTesting] = useState(false);
  const [config, setConfig] = useLocalStorage<SalesforceFormValues>('salesforceConfig', {
    username: "",
    password: "",
    securityToken: "",
    loginUrl: "https://test.salesforce.com/services/Soap/u/47.0"
  });

  const form = useForm<SalesforceFormValues>({
    resolver: zodResolver(salesforceSchema) as any,
    defaultValues: config
  });

  const testConnection = async () => {
    setIsTesting(true);
    try {
      const response = await fetch('/api/salesforce/test', {
        method: 'POST',
        body: JSON.stringify(form.getValues())
      });
      
      if (!response.ok) throw new Error(await response.text());
      toast.success("Connection successful!");
      return true;
    } catch (error: any) {
      toast.error("Connection failed: " + error.message);
      return false;
    } finally {
      setIsTesting(false);
    }
  };

  const onSubmit = async (data: SalesforceFormValues) => {
    const isConnected = await testConnection();
    if (isConnected) {
      setConfig(data);
      router.push('/pick-sf-fields');
    }
  };

  return (
    <div className="space-y-6 max-w-md">
      <h2 className="text-xl font-semibold">3. Salesforce Connection</h2>
      
      <Form {...form}>
        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
          <FormField control={form.control} name="username" render={({ field }) => (
            <FormItem><FormLabel>Username</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
          )}/>
          <FormField control={form.control} name="password" render={({ field }) => (
            <FormItem><FormLabel>Password</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
          )}/>
          <FormField control={form.control} name="securityToken" render={({ field }) => (
            <FormItem><FormLabel>Security Token</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
          )}/>
          <FormField control={form.control} name="loginUrl" render={({ field }) => (
            <FormItem><FormLabel>Login URL</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
          )}/>

          <div className="flex gap-4 pt-2">
            <Button type="button" variant="outline" onClick={() => router.back()}>Back</Button>
            <Button type="button" variant="outline" onClick={testConnection} disabled={isTesting}>
              {isTesting ? "Testing..." : "Test"}
            </Button>
            <Button type="submit" disabled={isTesting}>Continue</Button>
          </div>
        </form>
      </Form>
    </div>
  );
}