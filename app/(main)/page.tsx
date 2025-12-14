// app/(main)/page.tsx

import { Button } from '@/components/ui/button'
import Link from 'next/link'
import { Settings, Play } from 'lucide-react'

export default function Home() {
  return (
    <div className="max-w-2xl mx-auto py-12 space-y-8">
      <div className="text-center space-y-2">
        <h2 className="text-3xl font-bold">Account Matching with AI</h2>
        <p className="text-muted-foreground">
          Find duplicate accounts across Source CRM, Dimensions, and Salesforce
        </p>
      </div>
      
      <div className="flex flex-col gap-3">
        <Button asChild size="lg" className="h-16">
          <Link href="/databricks" className="flex items-center gap-3">
            <Settings className="h-5 w-5" />
            <div className="text-left">
              <div className="font-semibold">Configure & Connect</div>
              <div className="text-xs opacity-80">Set up Databricks and table paths</div>
            </div>
          </Link>
        </Button>
        
        <Button asChild variant="outline" size="lg" className="h-16" disabled>
          <Link href="/matching" className="flex items-center gap-3">
            <Play className="h-5 w-5" />
            <div className="text-left">
              <div className="font-semibold">Run Matching</div>
              <div className="text-xs opacity-80">Start AI-powered account matching</div>
            </div>
          </Link>
        </Button>
      </div>

      <div className="mt-12 p-6 bg-muted rounded-lg space-y-4">
        <h3 className="font-semibold text-lg">How it works:</h3>
        <ol className="space-y-3 text-sm">
          <li className="flex gap-3">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-xs font-bold">1</span>
            <span>Configure Databricks connection and specify your Source table path</span>
          </li>
          <li className="flex gap-3">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-xs font-bold">2</span>
            <span>System loads Source, Dimensions, and Salesforce accounts</span>
          </li>
          <li className="flex gap-3">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-xs font-bold">3</span>
            <span>AI analyzes matches handling variations in names, phones, domains, and addresses</span>
          </li>
          <li className="flex gap-3">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-xs font-bold">4</span>
            <span>Results show status: <code className="bg-background px-1 rounded text-xs">BOTH</code>, <code className="bg-background px-1 rounded text-xs">DIM_ONLY</code>, <code className="bg-background px-1 rounded text-xs">SF_ONLY</code>, or <code className="bg-background px-1 rounded text-xs">NEW</code></span>
          </li>
        </ol>
      </div>
    </div>
  )
}