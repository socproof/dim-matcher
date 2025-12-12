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
import { useState, useEffect } from 'react';
import { useLocalStorage } from "@/hooks/use-local-storage";
import { DatabricksConfig } from "@/app/api/databricks/client"; 


export default function ConnectionsPage() {
  const router = useRouter();
  const [isDatabricksTesting, setIsDatabricksTesting] = useState(false);
  const [isSalesforceTesting, setIsSalesforceTesting] = useState(false);
  const [areFormsValid, setAreFormsValid] = useState(false);

  // --- Databricks Setup ---
  const [databricksConfig, setDatabricksConfig] = useLocalStorage<DatabricksConfig>('databricksConfig', {
    apiUrl: "",
    accessToken: "",
    catalogName: "main", // Defaulting as per original
    schemaName: "default", // Defaulting as per original
    warehouseId: "",
    tableName: "", // Though not used in this form directly, it's part of the config
  });

  const databricksForm = useForm<DatabricksFormValues>({
    resolver: zodResolver(databricksSchema) as any, // The 'as any' is a common workaround for RHF+Zod typing issues
    defaultValues: databricksConfig,
    mode: 'onChange', // Validate on change to update button state
  });

  // --- Salesforce Setup ---
  const [salesforceConfig, setSalesforceConfig] = useLocalStorage<SalesforceFormValues>('salesforceConfig', {
    username: "",
    password: "",
    securityToken: "",
    loginUrl: "https://login.salesforce.com/services/Soap/u/58.0" // Use a more generic default or specific one like test
  });

  const salesforceForm = useForm<SalesforceFormValues>({
    resolver: zodResolver(salesforceSchema) as any,
    defaultValues: salesforceConfig,
    mode: 'onChange', // Validate on change to update button state
  });

  // Effect to monitor validity of both forms
  useEffect(() => {
    const dbValid = databricksForm.formState.isValid;
    const sfValid = salesforceForm.formState.isValid;
    setAreFormsValid(dbValid && sfValid);
  }, [databricksForm.formState.isValid, salesforceForm.formState.isValid]);


  const testDatabricksConnection = async () => {
    setIsDatabricksTesting(true);
    // Ensure latest values are used for testing, especially if not submitted yet
    await databricksForm.trigger(); // Trigger validation to ensure values are up-to-date if mode isn't 'onChange' for all fields
    if (!databricksForm.formState.isValid) {
        toast.error("Databricks form is invalid. Please fill required fields.");
        setIsDatabricksTesting(false);
        return false;
    }
    const values = databricksForm.getValues();
    try {
      const params = new URLSearchParams({
        apiUrl: values.apiUrl,
        accessToken: values.accessToken,
        catalogName: values.catalogName || 'main',
        schemaName: values.schemaName || 'default'
      });
  
      const response = await fetch(`/api/databricks/tables?${params.toString()}`, { // Ensure params is stringified
        method: 'HEAD' // Assuming HEAD is still the right method for your test endpoint
      });
  
      if (response.ok) {
        toast.success("Databricks connection successful!");
        return true;
      } else {
        const errorText = await response.text();
        throw new Error(errorText || 'Databricks connection failed with status: ' + response.status);
      }
    } catch (error: any) {
      toast.error("Databricks connection error: " + error.message);
      return false;
    } finally {
      setIsDatabricksTesting(false);
    }
  };

  const testSalesforceConnection = async () => {
    setIsSalesforceTesting(true);
    await salesforceForm.trigger();
    if (!salesforceForm.formState.isValid) {
        toast.error("Salesforce form is invalid. Please fill required fields.");
        setIsSalesforceTesting(false);
        return false;
    }
    const values = salesforceForm.getValues();
    try {
      const response = await fetch('/api/salesforce/test', { // Make sure this API endpoint exists
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values)
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || 'Salesforce connection failed with status: ' + response.status);
      }
      toast.success("Salesforce connection successful!");
      return true;
    } catch (error: any) {
      toast.error("Salesforce connection failed: " + error.message);
      return false;
    } finally {
      setIsSalesforceTesting(false);
    }
  };

  const handleSaveAndContinue = async () => {
    // Trigger validation for both forms explicitly
    const isDatabricksFormValid = await databricksForm.trigger();
    const isSalesforceFormValid = await salesforceForm.trigger();

    if (!isDatabricksFormValid || !isSalesforceFormValid) {
      toast.error("Please correct the errors in the forms.");
      return;
    }

    // Test connections one last time before saving
    // We can show a loading state for the main button here if desired
    const isDbConnected = await testDatabricksConnection();
    if (!isDbConnected) {
      toast.error("Databricks connection failed. Please check configuration and test again.");
      return;
    }

    const isSfConnected = await testSalesforceConnection();
    if (!isSfConnected) {
      toast.error("Salesforce connection failed. Please check configuration and test again.");
      return;
    }

    // If both connected
    if (isDbConnected && isSfConnected) {
      const dbValues = databricksForm.getValues();
      setDatabricksConfig({
        apiUrl: dbValues.apiUrl,
        accessToken: dbValues.accessToken,
        catalogName: dbValues.catalogName,
        schemaName: dbValues.schemaName,
        warehouseId: dbValues.warehouseId,
        tableName: databricksConfig.tableName,
      });

      const sfValues = salesforceForm.getValues();
      setSalesforceConfig(sfValues);
      
      toast.success("Configurations saved successfully!");
      router.push('/field-mapping');
    }
  };

  const isAnyTestRunning = isDatabricksTesting || isSalesforceTesting;

  return (
    <div className="container mx-auto p-4 space-y-8">
      <h1 className="text-2xl font-bold text-center mb-8">Configure Connections</h1>
      
      <div className="flex flex-col md:flex-row gap-8">
        {/* Databricks Form Column */}
        <div className="flex-1 p-6 border rounded-lg shadow-sm">
          <h2 className="text-xl font-semibold mb-4">1. Databricks Connection</h2>
          <Form {...databricksForm}>
            <form onSubmit={(e) => e.preventDefault()} className="space-y-4"> {/* Prevent default RHF submit */}
              <FormField
                control={databricksForm.control}
                name="apiUrl"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Workspace URL</FormLabel>
                    <FormControl>
                      <Input placeholder="https://<workspace>.cloud.databricks.com" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={databricksForm.control}
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
                control={databricksForm.control}
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
                control={databricksForm.control}
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
                control={databricksForm.control}
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
              <Button 
                type="button" 
                variant="outline"
                onClick={testDatabricksConnection}
                disabled={isDatabricksTesting || isSalesforceTesting} // Disable if any test is running
                className="w-full"
              >
                {isDatabricksTesting ? "Testing Databricks..." : "Test Databricks Connection"}
              </Button>
            </form>
          </Form>
        </div>

        {/* Salesforce Form Column */}
        <div className="flex-1 p-6 border rounded-lg shadow-sm">
          <h2 className="text-xl font-semibold mb-4">2. Salesforce Connection</h2>
          <Form {...salesforceForm}>
            <form onSubmit={(e) => e.preventDefault()} className="space-y-4"> {/* Prevent default RHF submit */}
              <FormField control={salesforceForm.control} name="username" render={({ field }) => (
                <FormItem><FormLabel>Username</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
              )}/>
              <FormField control={salesforceForm.control} name="password" render={({ field }) => (
                <FormItem><FormLabel>Password</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
              )}/>
              <FormField control={salesforceForm.control} name="securityToken" render={({ field }) => (
                <FormItem><FormLabel>Security Token</FormLabel><FormControl><Input type="password" {...field} /></FormControl><FormMessage /></FormItem>
              )}/>
              <FormField control={salesforceForm.control} name="loginUrl" render={({ field }) => (
                <FormItem><FormLabel>Login URL</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
              )}/>
              <Button 
                type="button" 
                variant="outline" 
                onClick={testSalesforceConnection} 
                disabled={isSalesforceTesting || isDatabricksTesting} // Disable if any test is running
                className="w-full"
              >
                {isSalesforceTesting ? "Testing Salesforce..." : "Test Salesforce Connection"}
              </Button>
            </form>
          </Form>
        </div>
      </div>

      {/* Combined Actions */}
      <div className="flex justify-center pt-6">
        <Button 
          type="button" 
          onClick={handleSaveAndContinue}
          disabled={isAnyTestRunning || !areFormsValid}
          className="px-8 py-3 text-lg"
        >
          {isAnyTestRunning ? "Testing..." : "Save & Continue"}
        </Button>
      </div>
    </div>
  );
}