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

        console.log('[AI Validate] ========== NEW REQUEST ==========');
        console.log('[AI Validate] Received pair IDs:', pairs.map(p => p.id).join(', '));
        console.log('[AI Validate] Pair details:');
        for (const pair of pairs) {
            console.log(`  ID=${pair.id}: "${pair.source.Name}" vs "${pair.target.Name}" (${pair.targetType})`);
        }

        const prompt = buildBatchPrompt(pairs);

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
                    temperature: 0,
                    num_predict: 200,
                },
            }),
        });

        if (!response.ok) {
            const error = await response.text();
            console.error('[AI Validate] Ollama error:', error);
            return NextResponse.json({ error: 'AI unavailable', details: error }, { status: 503 });
        }

        const data = await response.json();

        console.log('[AI Validate] ========== RAW RESPONSE START ==========');
        console.log(data.response);
        console.log('[AI Validate] ========== RAW RESPONSE END ==========');
        console.log('[AI Validate] Response length:', data.response.length);
        console.log('[AI Validate] First line:', data.response.split('\n')[0]);
        console.log('[AI Validate] Last line:', data.response.split('\n').slice(-1)[0]);

        const results = parseCSVResponse(data.response, pairs);  // ← FIX: pass pairs, not pairs.length

        return NextResponse.json({ results });
    } catch (error) {
        console.error('[AI Validate] Error:', error);
        return NextResponse.json({ error: 'Failed', details: String(error) }, { status: 500 });
    }
}

function buildBatchPrompt(pairs: AccountPair[]): string {
    let pairsText = '';

    for (const pair of pairs) {
        const sourceWebsite = pair.source.Website || 'N/A';
        const targetWebsite = pair.target.Website || 'N/A';

        const sourceDomain = sourceWebsite !== 'N/A' ? sourceWebsite.replace(/^(https?:\/\/)?(www\.)?/, '').split('/')[0].toLowerCase() : 'N/A';
        const targetDomain = targetWebsite !== 'N/A' ? targetWebsite.replace(/^(https?:\/\/)?(www\.)?/, '').split('/')[0].toLowerCase() : 'N/A';

        pairsText += `
---
ID: ${pair.id}
COMPANY_A: ${pair.source.Name || 'N/A'}
PHONE_A: ${pair.source.Phone || 'N/A'}
DOMAIN_A: ${sourceDomain}
CITY_A: ${pair.source.BillingCity || 'N/A'}
COUNTRY_A: ${pair.source.BillingCountry || 'N/A'}

COMPANY_B: ${pair.target.Name || 'N/A'}
PHONE_B: ${pair.target.Phone || 'N/A'}
DOMAIN_B: ${targetDomain}
CITY_B: ${pair.target.BillingCity || 'N/A'}
COUNTRY_B: ${pair.target.BillingCountry || 'N/A'}
`;
    }

    return `Compare these company pairs and return CSV format.

RULES:
- Same domain → YES, confidence 90
- Same phone → YES, confidence 90
- Same name + same city → YES, confidence 80
- Different countries + different domains → NO, confidence 85
- Different names → NO, confidence 80

PAIRS:
${pairsText}

REQUIRED OUTPUT FORMAT (one line per ID):
ID,DECISION,CONFIDENCE,REASON

DECISION: YES or NO (nothing else)
CONFIDENCE: number from 0 to 100 (just the number)
REASON: short text without commas

CORRECT EXAMPLES:
1,YES,95,Same domain example.com
2,NO,90,Different countries USA vs UK
3,YES,85,Same phone and city

WRONG EXAMPLES (do not do this):
1,NO;Different countries  ← WRONG (missing confidence number)
2,YES,90%+ Same domain ← WRONG (% symbol in confidence field)

Now analyze the pairs. Reply with ONLY CSV lines:`;
}

function parseCSVResponse(response: string, pairs: AccountPair[]): ValidationResult[] {
    const results: ValidationResult[] = [];
    const foundIds = new Set<number>();
    const expectedIds = new Set(pairs.map(p => p.id));

    let cleanResponse = response
        .replace(/```csv/gi, '')
        .replace(/```/g, '')
        .replace(/^[^0-9]+/gm, '')
        .trim();

    const lines = cleanResponse.split('\n');

    console.log('[parseCSVResponse] Expected IDs:', Array.from(expectedIds).join(', '));
    console.log('[parseCSVResponse] Cleaned lines:', lines);

    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        // Split by comma
        const parts = trimmed.split(',').map(p => p.trim());

        if (parts.length < 2) {
            console.warn('[parseCSVResponse] Skipping malformed line:', trimmed);
            continue;
        }

        const id = parseInt(parts[0]);
        if (isNaN(id)) {
            console.warn('[parseCSVResponse] Invalid ID in line:', trimmed);
            continue;
        }

        if (!expectedIds.has(id)) {
            console.warn(`[parseCSVResponse] AI returned unexpected ID ${id}, skipping`);
            continue;
        }

        if (foundIds.has(id)) {
            console.warn(`[parseCSVResponse] Duplicate ID ${id}, skipping`);
            continue;
        }

        const decision = parts[1].toUpperCase();
        const isMatch = decision === 'YES' || decision === 'Y';

        let confidence = 50;
        let reasoning = 'No reason provided';

        // Check if parts[2] is a number (confidence)
        if (parts.length >= 3) {
            const confStr = parts[2].replace(/%.*$/, '').trim(); // Remove % and everything after
            const parsedConf = parseInt(confStr);

            if (!isNaN(parsedConf) && parsedConf >= 0 && parsedConf <= 100) {
                // parts[2] is confidence
                confidence = parsedConf;
                reasoning = parts.slice(3).join(',').trim() || 'No reason provided';
            } else {
                // parts[2] is reasoning (AI skipped confidence)
                reasoning = parts.slice(2).join(',').trim();
                confidence = isMatch ? 70 : 85; // Default based on decision
            }
        }

        results.push({ id, isMatch, confidence, reasoning });
        foundIds.add(id);

        console.log(`[parseCSVResponse] ✓ Parsed ID=${id}, match=${isMatch}, conf=${confidence}, reason="${reasoning}"`);
    }

    // Add defaults for missing IDs
    const missingIds = Array.from(expectedIds).filter(id => !foundIds.has(id));

    if (missingIds.length > 0) {
        console.warn(`[parseCSVResponse] ❌ Missing results for IDs: ${missingIds.join(', ')}`);

        for (const id of missingIds) {
            results.push({
                id,
                isMatch: false,
                confidence: 0,
                reasoning: 'AI did not return result'
            });
        }
    }

    const sortedResults = results.sort((a, b) => a.id - b.id);

    console.log('[parseCSVResponse] Final:', sortedResults.map(r => `ID=${r.id}:${r.isMatch ? 'YES' : 'NO'}@${r.confidence}%`).join(', '));

    return sortedResults;
}