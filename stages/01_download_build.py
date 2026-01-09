#!/usr/bin/env python3
"""
Download and build TDC ADMET benchmark datasets.
Therapeutics Data Commons (TDC) provides curated ADMET datasets for ML.
Source: https://tdcommons.ai/
"""

import os
from pathlib import Path
import pandas as pd

# Import TDC
from tdc.single_pred import ADME, Tox

def main():
    brick_path = Path("brick")
    brick_path.mkdir(exist_ok=True)

    all_datasets = []

    # ADME datasets from TDC
    adme_datasets = [
        'Caco2_Wang',
        'PAMPA_NCATS',
        'HIA_Hou',
        'Pgp_Broccatelli',
        'Bioavailability_Ma',
        'Lipophilicity_AstraZeneca',
        'Solubility_AqSolDB',
        'HydrationFreeEnergy_FreeSolv',
        'CYP2C19_Veith',
        'CYP2D6_Veith',
        'CYP3A4_Veith',
        'CYP1A2_Veith',
        'CYP2C9_Veith',
        'CYP2C9_Substrate_CarbonMangels',
        'CYP2D6_Substrate_CarbonMangels',
        'CYP3A4_Substrate_CarbonMangels',
        'Half_Life_Obach',
        'Clearance_Hepatocyte_AZ',
        'Clearance_Microsome_AZ',
        'PPBR_AZ',
        'VDss_Lombardo',
        'BBB_Martins',
    ]

    # Toxicity datasets from TDC
    tox_datasets = [
        'hERG',
        'hERG_Karim',
        'AMES',
        'DILI',
        'Skin_Reaction',
        'Carcinogens_Lagunin',
        'ClinTox',
        'LD50_Zhu',
    ]

    print("=" * 60)
    print("Downloading ADME datasets from TDC...")
    print("=" * 60)

    for name in adme_datasets:
        print(f"\nProcessing ADME/{name}...")
        try:
            data = ADME(name=name)
            df = data.get_data()
            df['dataset'] = name
            df['category'] = 'ADME'

            # Standardize column names
            df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]

            # Save individual dataset
            output_file = brick_path / f"adme_{name.lower()}.parquet"
            df.to_parquet(output_file, index=False)
            print(f"  - Saved {len(df)} records to {output_file}")

            all_datasets.append(df)
        except Exception as e:
            print(f"  - Error: {e}")

    print("\n" + "=" * 60)
    print("Downloading Toxicity datasets from TDC...")
    print("=" * 60)

    for name in tox_datasets:
        print(f"\nProcessing Tox/{name}...")
        try:
            data = Tox(name=name)
            df = data.get_data()
            df['dataset'] = name
            df['category'] = 'Toxicity'

            # Standardize column names
            df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]

            # Save individual dataset
            output_file = brick_path / f"tox_{name.lower()}.parquet"
            df.to_parquet(output_file, index=False)
            print(f"  - Saved {len(df)} records to {output_file}")

            all_datasets.append(df)
        except Exception as e:
            print(f"  - Error: {e}")

    # Create combined summary
    print("\n" + "=" * 60)
    print("Creating combined dataset...")
    print("=" * 60)

    if all_datasets:
        # Create a summary with just key columns
        summary_data = []
        for df in all_datasets:
            cols_to_keep = ['drug_id', 'drug', 'y', 'dataset', 'category']
            available_cols = [c for c in cols_to_keep if c in df.columns]
            if available_cols:
                subset = df[available_cols].copy()
                # Convert drug_id to string to avoid mixed types
                if 'drug_id' in subset.columns:
                    subset['drug_id'] = subset['drug_id'].astype(str)
                summary_data.append(subset)

        if summary_data:
            combined = pd.concat(summary_data, ignore_index=True)
            combined_file = brick_path / "tdc_admet_combined.parquet"
            combined.to_parquet(combined_file, index=False)
            print(f"\nCombined dataset: {len(combined)} total records")

    # Print summary
    print("\n" + "=" * 60)
    print("Output files:")
    print("=" * 60)
    total_records = 0
    for f in sorted(brick_path.glob("*.parquet")):
        df = pd.read_parquet(f)
        total_records += len(df)
        print(f"  - {f.name}: {len(df)} rows")

    print(f"\nTotal: {total_records} records across {len(list(brick_path.glob('*.parquet')))} files")

if __name__ == "__main__":
    main()
