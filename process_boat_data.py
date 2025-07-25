#!/usr/bin/env python3
"""
Process compressed JSON boat data and convert to CSV files.

This script processes boat telemetry data stored in compressed JSON format,
extracting specific fields and creating separate CSV files for each message type.
"""

import json
import gzip
import os
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define gear state mapping
GEAR_STATE_MAP = {
    0: 'Neutral',
    1: 'Forward',
    2: 'Reverse',
    3: 'NotAvailable',
    255: 'Unknown'
}

# Define message type configurations
MESSAGE_CONFIGS = {
    'VolvoFuelLevel': {
        'message_type': 'PentaEngineStatus',
        'field': 'fuel_level.fuel_level1',
        'csv_name': 'fuel_level1.csv'
    },
    'EngineFuelEconomy': {
        'message_type': 'PentaEngineStatus',
        'field': 'fuel_econ.engine_fuel_rate',
        'csv_name': 'engine_fuel_rate.csv'
    },
    'EngineHours': {
        'message_type': 'PentaEngineStatus',
        'field': 'hours.engine_total_hours_of_operation',
        'csv_name': 'engine_total_hours_of_operation.csv'
    },
    'EngineTemperatures': {
        'message_type': 'PentaEngineStatus',
        'field': 'temps.engine_oil_temperature',
        'csv_name': 'engine_oil_temperature.csv'
    },
    'PentaEngineStatus_oil_pressure': {
        'message_type': 'PentaEngineStatus',
        'field': 'pressures.engine_oil_pressure',
        'csv_name': 'engine_oil_pressure.csv'
    },
    'EngineSpeed': {
        'message_type': 'PentaEngineStatus',
        'field': 'speed.engine_speed',
        'csv_name': 'engine_speed.csv'
    },
    'SeaState': {
        'message_type': 'SeaState',
        'field': 'instant_g_force',
        'csv_name': 'instant_g_force.csv'
    },
    'Odometry': {
        'message_type': 'Odometry',
        'field': 'odometer',
        'csv_name': 'odometer.csv'
    },
    'PentaEngineStatus_gear': {
        'message_type': 'PentaEngineStatus',
        'field': 'vessel_status.current_gear',
        'csv_name': 'current_gear.csv',
        'transform': lambda x: GEAR_STATE_MAP.get(x, f'Unknown({x})') if x is not None else None
    },
    'VehicleCommand': {
        'message_type': 'VehicleCommand',
        'field': 'throttle',
        'csv_name': 'throttle.csv'
    },
    'VesselHeading': {
        'message_type': 'VesselHeading',
        'field': 'heading',
        'csv_name': 'heading.csv',
        'transform': lambda x: x * 180.0 / 3.14159265359 if x is not None else None  # Convert to degrees
    },
    'PentaEngineStatus_steering': {
        'message_type': 'PentaEngineStatus',
        'field': 'vessel_status.current_steering_angle',
        'csv_name': 'current_steering_angle.csv'
    },
    'Ahrs_roll': {
        'message_type': 'Ahrs',
        'field': 'attitude.roll_deg',
        'csv_name': 'roll_deg.csv'
    },
    'Ahrs_pitch': {
        'message_type': 'Ahrs',
        'field': 'attitude.pitch_deg',
        'csv_name': 'pitch_deg.csv'
    },
    'Ahrs_yaw': {
        'message_type': 'Ahrs',
        'field': 'attitude.yaw_deg',
        'csv_name': 'yaw_deg.csv'
    }
}


def get_nested_value(data: Dict[str, Any], field_path: str) -> Any:
    """
    Extract a value from nested dictionary using dot notation.

    Args:
        data: Dictionary to extract from
        field_path: Dot-separated path to the field (e.g., 'attitude.roll_deg')

    Returns:
        The value at the specified path, or None if not found
    """
    fields = field_path.split('.')
    value = data

    for field in fields:
        if isinstance(value, dict) and field in value:
            value = value[field]
        else:
            return None

    return value


def read_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Read and parse a JSON/JSONL file.

    Args:
        file_path: Path to the JSON file

    Returns:
        List of parsed JSON records
    """
    records = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON line in {file_path}: {e}")
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")

    return records


def process_boat_day_data(day_dir: Path, output_dir: Path, boat_name: str) -> None:
    """
    Process all JSON files in a boat's day directory.

    Args:
        day_dir: Path to the day directory containing JSON files
        output_dir: Path to output directory for CSV files
        boat_name: Name of the boat (e.g., 'cr38')
    """
    # Initialize data collectors for each message type
    message_data = {config_key: [] for config_key in MESSAGE_CONFIGS.keys()}

    # Find all JSON files in the directory (excluding .zst files)
    json_files = []
    for pattern in ['*.json', '*.jsonl']:
        json_files.extend(day_dir.glob(pattern))

    # Filter out any .zst files that might have been picked up
    json_files = [f for f in json_files if not str(f).endswith('.zst')]

    if not json_files:
        logger.warning(f"No JSON files found in {day_dir}")
        return

    logger.info(f"Processing {len(json_files)} JSON files in {day_dir}")

    # Process each JSON file
    for json_file in json_files:
        logger.debug(f"Processing file: {json_file}")
        records = read_json_file(json_file)

        # Process each record
        for record in records:
            # Get timestamp from 'ts' field
            timestamp = record.get('ts', '')

            # Get the message object from 'msg' field
            msg_obj = record.get('msg', {})

            # The message type is the key inside the msg object
            if msg_obj:
                # Get the first (and only) key in the msg object
                message_type = list(msg_obj.keys())[0] if msg_obj else ''
                message_data_obj = msg_obj.get(message_type, {})

                # Process each configured message type
                for config_key, config in MESSAGE_CONFIGS.items():
                    # Determine the actual message type to look for
                    check_message_type = config.get('message_type', config_key)

                    if message_type == check_message_type:
                        # Extract the field value from the message data object
                        field_value = get_nested_value(message_data_obj, config['field'])

                        # Apply transformation if specified
                        if 'transform' in config and field_value is not None:
                            field_value = config['transform'](field_value)

                        # Store the data point
                        if field_value is not None:
                            message_data[config_key].append({
                                'boat': boat_name,
                                'value': field_value,
                                'timestamp': timestamp
                            })

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write CSV files for each message type
    for config_key, config in MESSAGE_CONFIGS.items():
        data_points = message_data[config_key]

        if data_points:
            csv_path = output_dir / config['csv_name']

            with open(csv_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['boat', 'value', 'timestamp'])
                writer.writeheader()

                # Sort by timestamp before writing
                data_points.sort(key=lambda x: x['timestamp'])
                writer.writerows(data_points)

            logger.info(f"Created {csv_path} with {len(data_points)} data points")
        else:
            logger.warning(f"No data found for {config_key}")


def process_all_boats(raw_json_dir: Path, output_base_dir: Path, boat_filter: Optional[str] = None, day_filter: Optional[str] = None) -> None:
    """
    Process all boats in the raw_json directory.

    Args:
        raw_json_dir: Path to the raw_json directory
        output_base_dir: Base directory for output CSV files
        boat_filter: Optional boat name to process only that boat
        day_filter: Optional day name to process only that day
    """
    if not raw_json_dir.exists():
        logger.error(f"Raw JSON directory does not exist: {raw_json_dir}")
        return

    # Find all boat directories
    boat_dirs = [d for d in raw_json_dir.iterdir() if d.is_dir()]

    if not boat_dirs:
        logger.warning(f"No boat directories found in {raw_json_dir}")
        return

    # Filter boats if requested
    if boat_filter:
        boat_dirs = [d for d in boat_dirs if d.name == boat_filter]
        if not boat_dirs:
            logger.error(f"Boat '{boat_filter}' not found in {raw_json_dir}")
            return
        logger.info(f"Processing single boat: {boat_filter}")
    else:
        logger.info(f"Found {len(boat_dirs)} boat directories")

    # Process each boat
    for boat_dir in boat_dirs:
        boat_name = boat_dir.name
        logger.info(f"Processing boat: {boat_name}")

        # Find all day directories within the boat directory
        day_dirs = [d for d in boat_dir.iterdir() if d.is_dir()]

        if not day_dirs:
            logger.warning(f"No day directories found for boat {boat_name}")
            continue

        # Filter days if requested
        if day_filter:
            day_dirs = [d for d in day_dirs if d.name == day_filter]
            if not day_dirs:
                logger.warning(f"Day '{day_filter}' not found for boat {boat_name}")
                continue
            logger.info(f"Processing single day: {day_filter}")

        # Process each day
        for day_dir in day_dirs:
            day_name = day_dir.name
            logger.info(f"Processing day: {day_name} for boat: {boat_name}")

            # Create output directory for this boat/day
            output_dir = output_base_dir / boat_name / day_name

            # Process the data
            process_boat_day_data(day_dir, output_dir, boat_name)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Process compressed JSON boat data and convert to CSV files'
    )
    parser.add_argument(
        'raw_json_dir',
        type=Path,
        help='Path to the raw_json directory containing boat data'
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=Path('output_csv'),
        help='Output directory for CSV files (default: output_csv)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '-b', '--boat',
        type=str,
        help='Process only a specific boat (by directory name)'
    )
    parser.add_argument(
        '-d', '--day',
        type=str,
        help='Process only a specific day (by directory name)'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Process all boats
    logger.info(f"Starting processing of boat data from {args.raw_json_dir}")
    logger.info(f"Output will be saved to {args.output}")

    process_all_boats(args.raw_json_dir, args.output, args.boat, args.day)

    logger.info("Processing complete!")


if __name__ == '__main__':
    main()
