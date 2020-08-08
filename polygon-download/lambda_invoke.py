#!/usr/bin/env python3
"""Invokes the Lambda function async using an event loaded from a JSON file.

Example:
    ./lambda_invoke.py --lambda_event_file lambda_event.json

"""
import argparse

import boto3


def main() -> None:
    """Start execution.

    """
    client = boto3.client('lambda')

    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--lambda_event_file',
                        metavar='FILE',
                        help='Lambda event JSON',
                        default='lambda_event.json')
    args = parser.parse_args()

    # Load Lambda event JSON file.
    with open(args.lambda_event_file, 'r') as json_file:
        json_string = json_file.read()

    # Invoke Lambda funtion async.
    response = client.invoke(
        FunctionName='polygon_download',
        InvocationType='Event',
        LogType='Tail',
        # ClientContext='',
        Payload=json_string.encode(),
        # Qualifier=''
    )

    # Log response.
    print(response)


if __name__ == '__main__':
    main()
