import { NextRequest, NextResponse } from 'next/server';

const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434';
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || 'deepseek-v3.1:latest';

interface AccountPair {
    id: number;
    source: Record<string, any>;
    target: Record<string, any>;
    score: number;
    matchedFields: string[];
    targetType: 'dimensions' | 'salesforce';
}

interface BatchValidationRequest {
    pairs: AccountPair[];
}

interface ValidationResult {
    id: number;
    isMatch: boolean;
    confidence: number;
    reasoning: string;
}

export async function POST(request: NextRequest) {
    try {
        const body: BatchValidationRequest = await request.json();
        const { pairs } = body;

        if (!pairs || pairs.length === 0) {
            return NextResponse.json({ results: [] });
        }

        const prompt = buildBatchPrompt(pairs);

        // Log prompt for debugging
        console.log('[AI Validate] Prompt length:', prompt.length);
        console.log('[AI Validate] First 500 chars:', prompt.substring(0, 500));

        const response = await fetch(`${OLLAMA_URL}/api/generate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                model: OLLAMA_MODEL,
                prompt,
                stream: false,
                options: {
                    temperature: 0,      // Deterministic
                    num_predict: 200,    // Enough for CSV output
                },
            }),
        });

        if (!response.ok) {
            const error = await response.text();
            console.error('[AI Validate] Ollama error:', error);
            return NextResponse.json({ error: 'AI unavailable', details: error }, { status: 503 });
        }

        const data = await response.json();

        // Log raw response for debugging
        console.log('[AI Validate] Raw AI response:');
        console.log(data.response);
        console.log('[AI Validate] ---end of response---');

        const results = parseCSVResponse(data.response, pairs.length);

        return NextResponse.json({ results });
    } catch (error) {
        console.error('[AI Validate] Error:', error);
        return NextResponse.json({ error: 'Failed', details: String(error) }, { status: 500 });
    }
}

function buildBatchPrompt(pairs: AccountPair[]): string {
    let pairsText = '';

    for (const pair of pairs) {
        pairsText += `
---
ID: ${pair.id}
COMPANY_A: ${pair.source.Name || 'N/A'}
PHONE_A: ${pair.source.Phone || 'N/A'}
WEBSITE_A: ${pair.source.Website || 'N/A'}
ADDRESS_A: ${[pair.source.BillingStreet, pair.source.BillingCity, pair.source.BillingPostalCode, pair.source.BillingCountry].filter(Boolean).join(', ') || 'N/A'}

COMPANY_B: ${pair.target.Name || 'N/A'}
PHONE_B: ${pair.target.Phone || 'N/A'}
WEBSITE_B: ${pair.target.Website || 'N/A'}
ADDRESS_B: ${[pair.target.BillingStreet, pair.target.BillingCity, pair.target.BillingPostalCode, pair.target.BillingCountry].filter(Boolean).join(', ') || 'N/A'}

HEURISTIC_SCORE: ${pair.score}
`;
    }

    return `You are a business data matching expert. Determine if each pair represents the SAME business entity.

IMPORTANT RULES:
1. Same website domain = SAME company (even with different addresses - could be multiple offices)
2. Same phone number = SAME company  
3. Similar name + same city = likely SAME company
4. Similar name + DIFFERENT city/country = DIFFERENT companies
5. Company suffixes (Ltd, Inc, Pty, Limited) should be IGNORED when comparing names
6. Different addresses in same city could be branch offices = still SAME company

PAIRS TO ANALYZE:
${pairsText}

Respond with EXACTLY one line per pair in this CSV format:
ID,DECISION,CONFIDENCE,REASON

Where:
- ID: the pair number
- DECISION: YES (same company) or NO (different companies)
- CONFIDENCE: 0-100 (how sure you are)
- REASON: brief explanation (no commas allowed in reason)

Example response:
1,YES,95,Same website confirms identical business
2,NO,85,Same name but different countries
3,YES,80,Matching phone number and similar address

YOUR RESPONSE (only CSV lines):`;
}

function parseCSVResponse(response: string, expectedCount: number): ValidationResult[] {
    const results: ValidationResult[] = [];

    // Clean up response - remove markdown, extra text
    let cleanResponse = response
        .replace(/```csv/gi, '')
        .replace(/```/g, '')
        .replace(/^[^0-9]+/gm, '') // Remove lines not starting with numbers
        .trim();

    const lines = cleanResponse.split('\n');

    console.log('[parseCSVResponse] Cleaned lines:', lines);

    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        // Match pattern: ID,YES/NO,NUMBER,TEXT
        const match = trimmed.match(/^(\d+)\s*,\s*(YES|NO)\s*,\s*(\d+)\s*,\s*(.+)$/i);

        if (match) {
            const id = parseInt(match[1]);
            const isMatch = match[2].toUpperCase() === 'YES';
            const confidence = Math.min(100, Math.max(0, parseInt(match[3])));
            const reasoning = match[4].trim();

            results.push({ id, isMatch, confidence, reasoning });
            console.log(`[parseCSVResponse] Parsed: id=${id}, match=${isMatch}, conf=${confidence}`);
        } else {
            // Try alternative parsing for malformed lines
            const parts = trimmed.split(',');
            if (parts.length >= 3) {
                const id = parseInt(parts[0]);
                if (!isNaN(id)) {
                    const decision = parts[1]?.toUpperCase().trim();
                    const isMatch = decision === 'YES' || decision === 'Y' || decision === 'TRUE';
                    const confidence = parseInt(parts[2]) || 50;
                    const reasoning = parts.slice(3).join(' ').trim() || 'No reason provided';

                    results.push({ id, isMatch, confidence, reasoning });
                    console.log(`[parseCSVResponse] Fallback parsed: id=${id}, match=${isMatch}`);
                }
            }
        }
    }

    // Fill missing results
    for (let i = 1; i <= expectedCount; i++) {
        if (!results.find(r => r.id === i)) {
            console.log(`[parseCSVResponse] Missing result for id=${i}, adding default`);
            results.push({
                id: i,
                isMatch: false,
                confidence: 0,
                reasoning: 'AI did not return result for this pair'
            });
        }
    }

    return results.sort((a, b) => a.id - b.id);
}