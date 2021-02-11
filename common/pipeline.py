#!/usr/bin/env python3
"""Run the entire reup pipeline.

"""
import reup_utils
import polygon_tick_data.lambda_invoke as ptd_lambda_invoke

# universe = reup_utils.Universe('reup', 'universes/russell-3000')
# print(universe.get_symbol_df('2019-08-08').head())
# print(universe.get_symbol_list('2019-08-08')[0:10])

date_symbol_dict = {
    '2020-01-02': ['GOOG', 'SPY'],
    '2020-01-03': ['GOOG', 'SPY']
}
# lambda_invoke = reup_utils.LambdaInvokeSimple(
lambda_invoke = ptd_lambda_invoke.LambdaInvoke(
    '../polygon_tick_data/polygon_tick_data/lambda_invoke_config.yaml',
    '../polygon_tick_data/polygon_tick_data/lambda_event.json')
pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    date_symbol_dict)
print(pending_date_symbol_dict)
if len(pending_date_symbol_dict) > 0:
    lambda_invoke.run(pending_date_symbol_dict)
