import asyncio
import json
from pathlib import Path

from src.student_agent.cases import load_case_set
from src.student_agent.config import Settings
from src.student_agent.contracts import Contracts
from src.student_agent.mcp_gateway import connect_gateway


async def main():
    root = Path('.').resolve()
    settings = Settings.load(root)
    contracts = Contracts(root / 'contracts' / 'schemas')
    cases = load_case_set(root).cases
    async with connect_gateway(settings.mcp_endpoint, settings.team_api_key, contracts) as gateway:
        for case_id in ['L3B_CASE_004','L3B_CASE_005','L3B_CASE_006','L3B_CASE_007','L3B_CASE_008','L3B_CASE_010']:
            case = cases[case_id]
            order_id = case.get('customer_request', {}).get('claimed_order_id')
            print('\nCASE', case_id, 'topics=', [x.get('topic') for x in case['customer_request']['claims']], 'order=', order_id)
            for tool, args in [
                ('get_order', {'order_id': order_id}),
                ('get_order_payments', {'order_id': order_id}),
                ('get_payment_timeline', {'order_id': order_id}),
                ('get_refund_timeline', {'order_id': order_id}),
                ('get_shipment_summary', {'order_id': order_id}),
            ]:
                try:
                    e = await gateway.call(tool, case_id=case_id, **args)
                    print(tool, json.dumps(e.get('data'), ensure_ascii=False, sort_keys=True))
                except Exception as exc:
                    print(tool, 'ERROR', type(exc).__name__, str(exc))


asyncio.run(main())
