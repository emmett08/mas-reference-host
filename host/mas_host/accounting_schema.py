"""Period and provider-observation additions to the deployment ledger."""

ACCOUNTING_TABLES = (
    'CREATE TABLE period_policies(scope TEXT PRIMARY KEY,policy TEXT NOT NULL,high_water INTEGER NOT NULL,request_key TEXT UNIQUE NOT NULL,actor TEXT NOT NULL,reason TEXT NOT NULL,created INTEGER NOT NULL)',
    'CREATE TABLE period_allocations(scope TEXT NOT NULL,period INTEGER NOT NULL,run_id TEXT NOT NULL,call_id TEXT NOT NULL,reserved_microusd INTEGER NOT NULL,PRIMARY KEY(scope,run_id,call_id),FOREIGN KEY(scope) REFERENCES period_policies(scope))',
    'CREATE INDEX period_usage ON period_allocations(scope,period)',
    'CREATE TABLE provider_charges(provider TEXT NOT NULL,charge_id TEXT NOT NULL,run_id TEXT NOT NULL,call_id TEXT NOT NULL,actual_microusd INTEGER NOT NULL,observed_at INTEGER NOT NULL,evidence TEXT NOT NULL,actor TEXT NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(provider,charge_id),FOREIGN KEY(run_id,call_id) REFERENCES calls(run_id,call_id))',
)
