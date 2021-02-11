#!/usr/bin/env python3
"""Run the entire reup pipeline.

"""
import reup_utils
import polygon_tick_data.lambda_invoke as ptd_lambda_invoke
import polygon_time_series.lambda_invoke as pts_lambda_invoke


# universe = reup_utils.Universe('reup', 'universes/russell-3000')
# print(universe.get_symbol_df('2019-08-08').head())
# print(universe.get_symbol_list('2019-08-08')[0:10])
date_symbol_dict = {
    '2020-01-02': ['GOOG', 'SPY'],
    '2020-01-03': ['GOOG', 'SPY']
}


# # Run polygon tick data.
# lambda_invoke = ptd_lambda_invoke.LambdaInvoke(
#     '../polygon_tick_data/polygon_tick_data/lambda_invoke_config.yaml',
#     '../polygon_tick_data/polygon_tick_data/lambda_event.json')
# pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
#     date_symbol_dict)
# print(pending_date_symbol_dict)
# if len(pending_date_symbol_dict) > 0:
#     lambda_invoke.run(pending_date_symbol_dict)


# # Run polygon time series.
# lambda_invoke = pts_lambda_invoke.LambdaInvoke(
#     '../polygon_time_series/polygon_time_series/lambda_invoke_config.yaml',
#     '../polygon_time_series/polygon_time_series/lambda_event.json')
# pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
#     date_symbol_dict)
# print(pending_date_symbol_dict)
# if len(pending_date_symbol_dict) > 0:
#     lambda_invoke.run(pending_date_symbol_dict)


# # Run features second.
# lambda_invoke = reup_utils.LambdaInvokeSimple(
#     '../features_second/features_second/lambda_invoke_config.yaml',
#     '../features_second/features_second/lambda_event.json')
# pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
#     date_symbol_dict)
# print(pending_date_symbol_dict)
# if len(pending_date_symbol_dict) > 0:
#     lambda_invoke.run(pending_date_symbol_dict)


# Run features day.
lambda_invoke = reup_utils.LambdaInvokeSimple(
    '../features_day/features_day/lambda_invoke_config.yaml',
    '../features_day/features_day/lambda_event.json')
pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    date_symbol_dict)
print(pending_date_symbol_dict)
if len(pending_date_symbol_dict) > 0:
    lambda_invoke.run(pending_date_symbol_dict)

