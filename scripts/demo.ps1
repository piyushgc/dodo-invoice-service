# Step-by-step demo for the video. Run from the project folder:
#   pwsh -ExecutionPolicy Bypass -File scripts/demo.ps1            (API on port 8080)
#   pwsh -ExecutionPolicy Bypass -File scripts/demo.ps1 -Port 8090 (if 8080 is taken)
# Press Enter between steps. Watch webhooks in another terminal:
#   docker compose logs -f webhook-sink
param(
    [int]$Port = 8080,
    [switch]$NoPause
)

$ErrorActionPreference = "Stop"
$Api = "http://localhost:$Port"
$Run = Get-Random -Maximum 99999   # unique keys per run, so the demo can be repeated
$Auth = @{ Authorization = "Bearer sk_test_demo_key_local_only" }

function Step($title) {
    if (-not $NoPause) { Read-Host "`nPress Enter for: $title" | Out-Null }
    Write-Host "`n==== $title ====" -ForegroundColor Cyan
}

function Call($method, $path, $body = $null, $extraHeaders = @{}) {
    $headers = $Auth + $extraHeaders
    Write-Host "$method $path" -ForegroundColor Yellow
    foreach ($k in $extraHeaders.Keys) { Write-Host "  $($k): $($extraHeaders[$k])" -ForegroundColor DarkYellow }
    $params = @{ Method = $method; Uri = "$Api$path"; Headers = $headers; SkipHttpErrorCheck = $true }
    if ($null -ne $body) {
        $json = $body | ConvertTo-Json -Depth 10 -Compress
        Write-Host "  body: $json" -ForegroundColor DarkYellow
        $params.Body = $json
        $params.ContentType = "application/json"
    }
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $resp = Invoke-WebRequest @params
    $sw.Stop()
    $color = if ($resp.StatusCode -lt 300) { "Green" } else { "Red" }
    Write-Host "-> HTTP $($resp.StatusCode)  ($([int]$sw.Elapsed.TotalMilliseconds) ms)" -ForegroundColor $color
    if ($resp.Headers["Idempotent-Replayed"]) { Write-Host "-> Idempotent-Replayed: true" -ForegroundColor Magenta }
    $obj = $resp.Content | ConvertFrom-Json
    Write-Host ($obj | ConvertTo-Json -Depth 10)
    return $obj
}

function NewInvoice($customerId, $items) {
    return Call POST "/v1/invoices" @{ customer_id = $customerId; due_date = "2026-12-31"; line_items = $items }
}

# --- sanity check ---
try { Invoke-WebRequest "$Api/healthz" -SkipHttpErrorCheck | Out-Null }
catch { Write-Host "API is not reachable at $Api. Is 'docker compose up' running?" -ForegroundColor Red; exit 1 }

Step "1. Create a customer"
$customer = Call POST "/v1/customers" @{ name = "Jane Doe"; email = "jane@example.com" }

Step "2. Create an invoice (server computes total: 3 x 15000 + 1 x 2999 = 47999 cents)"
$inv1 = NewInvoice $customer.id @(
    @{ description = "Consulting"; quantity = 3; unit_amount_cents = 15000 },
    @{ description = "Hosting"; quantity = 1; unit_amount_cents = 2999 })

Step "3. Pay it with tok_success"
Call POST "/v1/invoices/$($inv1.id)/pay" @{ card_token = "tok_success" } @{ "Idempotency-Key" = "demo-pay-1-$Run" } | Out-Null

Step "4. Send the SAME request again (same Idempotency-Key) -> same answer, PSP not called again"
Call POST "/v1/invoices/$($inv1.id)/pay" @{ card_token = "tok_success" } @{ "Idempotency-Key" = "demo-pay-1-$Run" } | Out-Null

Step "5. New invoice, pay with tok_card_declined -> 402, invoice stays open"
$inv2 = NewInvoice $customer.id @(@{ description = "Widget"; quantity = 2; unit_amount_cents = 1250 })
Call POST "/v1/invoices/$($inv2.id)/pay" @{ card_token = "tok_card_declined" } @{ "Idempotency-Key" = "demo-pay-2-$Run" } | Out-Null

Step "6. Try to VOID the paid invoice -> 409 invalid_state_transition"
Call POST "/v1/invoices/$($inv1.id)/void" | Out-Null

Step "7. Webhook events and their delivery status"
$events = Invoke-RestMethod "$Api/v1/events?limit=10" -Headers $Auth
$events.data | ForEach-Object {
    $d = $_.deliveries | Select-Object -First 1
    "{0,-26} delivery: {1,-10} attempts: {2}" -f $_.type, $d.status, $d.attempt_count
} | Write-Host

Step "8. FAILURE MODE: new invoice, pay with tok_timeout (PSP sleeps 30s)"
$inv3 = NewInvoice $customer.id @(@{ description = "Slow one"; quantity = 1; unit_amount_cents = 5000 })
Call POST "/v1/invoices/$($inv3.id)/pay" @{ card_token = "tok_timeout" } @{ "Idempotency-Key" = "demo-pay-3-$Run" } | Out-Null
Write-Host "Returned 202 after ~5s: we did not hang, and we did not guess the result." -ForegroundColor Magenta

Step "9. While pending, a second payment is refused -> 409 payment_in_progress (no double charge)"
Call POST "/v1/invoices/$($inv3.id)/pay" @{ card_token = "tok_success" } @{ "Idempotency-Key" = "demo-pay-4-$Run" } | Out-Null

Step "10. Wait for the reconciler to settle the pending payment (~30s)"
$deadline = (Get-Date).AddSeconds(90)
do {
    Start-Sleep -Seconds 3
    $status = (Invoke-RestMethod "$Api/v1/invoices/$($inv3.id)" -Headers $Auth).status
    Write-Host ("{0:HH:mm:ss}  invoice status: {1}" -f (Get-Date), $status)
} until ($status -ne "open" -or (Get-Date) -gt $deadline)

Step "11. Retry the ORIGINAL timed-out request (same key) -> now returns the final result"
Call POST "/v1/invoices/$($inv3.id)/pay" @{ card_token = "tok_timeout" } @{ "Idempotency-Key" = "demo-pay-3-$Run" } | Out-Null

Write-Host "`nDemo finished." -ForegroundColor Cyan
