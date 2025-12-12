"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { databricksSchema, DatabricksFormValues } from "@/lib/databricks-schema";
import { salesforceSchema, SalesforceFormValues } from "@/lib/salesforce-schema";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { useRouter } from 'next/navigation';
import { toast } from 'sonner';
import { useState } from 'react';
import { useLocalStorage } from "@/hooks/use-local-storage";
import { DatabricksConfig } from "@/app/api/databricks/client"; // Assuming this is used by DatabricksFormValues implicitly or by the hook

// Salesforce might need its own config type if SalesforceFormValues isn't sufficient for useLocalStorage
// For now, using SalesforceFormValues as the type for useLocalStorage for Salesforce.

export default function ConnectionsPage() {
  const router = useRouter();

  // --- Databricks State & Logic ---
  const [isTestingDatabricks, setIsTestingDatabricks] = useState(false);
  const [databricksConfig, setDatabricksConfig] = useLocalStorage<DatabricksConfig>('databricksConfig', {
    apiUrl: "",
    accessToken: "",
    catalogName: "",
    schemaName: "",
    warehouseId: "",
    tableName: "", // tableName might not be needed here based on the original handleSubmit
  });

  const formDatabricks = useForm<DatabricksFormValues>({
    resolver: zodResolver(databricksSchema) as any, // Added 'as any' to match original
    defaultValues: databricksConfig
  });

  const testDatabricksConnection = async () => {
    setIsTestingDatabricks(true);
    try {
      const values = formDatabricks.getValues();
      const params = new URLSearchParams({
        apiUrl: values.apiUrl,
        accessToken: values.accessToken,
        catalogName: values.catalogName || 'main',
        schemaName: values.schemaName || 'default'
      });
  
      const response = await fetch(`/api/databricks/tables?${params}`, {
        method: 'HEAD'
      });
  
      if (response.ok) {
        toast.success("Databricks connection successful!");
        return true;
      } else {
        const error = await response.text();
        throw new Error(error || 'Databricks connection failed');
      }
    } catch (error: any) {
      toast.error("Databricks connection error: " + error.message);
      return false;
    } finally {
      setIsTestingDatabricks(false);
    }
  };

  const handleDatabricksSubmit = async (data: DatabricksFormValues) => {
    const isConnected = await testDatabricksConnection();
    if (isConnected) {
      setDatabricksConfig({ // Ensure all fields from DatabricksConfig are set
        apiUrl: data.apiUrl,
        accessToken: data.accessToken,
        catalogName: data.catalogName,
        schemaName: data.schemaName,
        warehouseId: data.warehouseId,
        tableName: databricksConfig.tableName, // Preserve tableName or handle as needed
      });
      toast.success("Databricks configuration saved!");
      router.push('/pick-db-fields');
    }
  };

  // --- Salesforce State & Logic ---
  const [isTestingSalesforce, setIsTestingSalesforce] = useState(false);
  const [salesforceConfig, setSalesforceConfig] = useLocalStorage<SalesforceFormValues>('salesforceConfig', {
    username: "",
    password: "",
    securityToken: "",
    loginUrl: "https://test.salesforce.com/services/Soap/u/47.0"
  });

  const formSalesforce = useForm<SalesforceFormValues>({
    resolver: zodResolver(salesforceSchema) as any, // Added 'as any' to match original
    defaultValues: salesforceConfig
  });

  const testSalesforceConnection = async () => {
    setIsTestingSalesforce(true);
    try {
      const response = await fetch('/api/salesforce/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }, // Good practice to add headers
        body: JSON.stringify(formSalesforce.getValues())
      });
      
      if (!response.ok) throw new Error(await response.text());
      toast.success("Salesforce connection successful!");
      return true;
    } catch (error: any) {
      toast.error("Salesforce connection failed: " + error.message);
      return false;
    } finally {
      setIsTestingSalesforce(false);
    }
  };

  const handleSalesforceSubmit = async (data: SalesforceFormValues) => {
    const isConnected = await testSalesforceConnection();
    if (isConnected) {
      setSalesforceConfig(data);
      toast.success("Salesforce configuration saved!");
      router.push('/pick-sf-fields');
    }
  };

  return (
    <div className="space-y-12 max-w-md mx-auto py-10"> {/* Added mx-auto and py-10 for centering and padding */}
      
      {/* Databricks Section */}
      <section className="space-y-6">
        <h2 className="text-xl font-semibold">1. Databricks Connection</h2>
        
        <Form {...formDatabricks}>
          <form onSubmit={formDatabricks.handleSubmit(handleDatabricksSubmit)} className="space-y-4">
            <FormField
              control={formDatabricks.control}
              name="apiUrl"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Workspace URL</FormLabel>
                  <FormControl>
                    <Input 
                      placeholder="https://<workspace>.cloud.databricks.com" 
                      {...field} 
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            
            <FormField
              control={formDatabricks.control}
              name="accessToken"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Access Token</FormLabel>
                  <FormControl>
                    <Input type="password" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={formDatabricks.control}
              name="catalogName"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Catalog Name</FormLabel>
                  <FormControl>
                    <Input placeholder="main" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={formDatabricks.control}
              name="schemaName"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Schema Name</FormLabel>
                  <FormControl>
                    <Input placeholder="default" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={formDatabricks.control}
              name="warehouseId"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Warehouse ID</FormLabel>
                  <FormControl>
                    <Input placeholder="1234a567b890c12d" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div className="flex gap-4 pt-2">
              <Button 
                type="button" 
                variant="outline"
                onClick={testDatabricksConnection}
                disabled={isTestingDatabricks}
              >
                {isTestingDatabricks ? "Testing..." : "Test Databricks"}
              </Button>
              
              <Button 
                type="submit" 
                disabled={isTestingDatabricks}
              >
                Save Databricks & Continue
              </Button>
            </div>
          </form>
        </Form>
      </section>

      {/* Salesforce Section */}
      <section className="space-y-6">
        <h2 className="text-xl font-semibold">2. Salesforce Connection</h2> {/* Adjusted numbering */}
        
        <Form {...formSalesforce}>
          <form onSubmit={formSalesforce.handleSubmit(handleSalesforceSubmit)} className="space-y-4">
            <FormField control={formSalesforce.control} name="username" render={({ field }) => (
              <FormItem><FormLabel>Username</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
            )}/>
            <FormField control={formSalesforce.control} name="password" render={({ field }) => (
              <FormItem><FormLabel>Password</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
            )}/>
            <FormField control={formSalesforce.control} name="securityToken" render={({ field }) => (
              <FormItem><FormLabel>Security Token</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
            )}/>
            <FormField control={formSalesforce.control} name="loginUrl" render={({ field }) => (
              <FormItem><FormLabel>Login URL</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
            )}/>

            <div className="flex gap-4 pt-2">
              {/* Removed the original "Back" button as its context is different now */}
              <Button type="button" variant="outline" onClick={testSalesforceConnection} disabled={isTestingSalesforce}>
                {isTestingSalesforce ? "Testing..." : "Test Salesforce"}
              </Button>
              <Button type="submit" disabled={isTestingSalesforce}>
                Save Salesforce & Continue
              </Button>
            </div>
          </form>
        </Form>
      </section>

    </div>
  );
}