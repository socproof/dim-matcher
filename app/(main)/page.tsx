import { Button } from '@/components/ui/button'
import Link from 'next/link'

export default function Home() {
  return (
    <div className="space-y-4">
      <h2 className="text-xl font-semibold">Data Matching POC</h2>
      <div className="flex flex-col space-y-2">
        <Button asChild>
          <Link href="/connections">1. Configure connection</Link>
        </Button>
        <Button asChild variant="outline" disabled>
          <Link href="/field-mapping">2. Map fields</Link>
        </Button>
        <Button asChild variant="outline" disabled>
          <Link href="/matching">3. Search for matchings</Link>
        </Button>
      </div>
    </div>
  )
}