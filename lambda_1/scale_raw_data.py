
import csv
import argparse
import os
import sys

def scale_csv_data(input_file, output_file, multiplier):
    print(f"Reading {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        original_rows = list(reader)
    
    original_count = len(original_rows)
    print(f"Original records: {original_count}")
    
    
    max_id = 0
    for row in original_rows:
        try:
            row_id = int(row.get('id', 0))
            if row_id > max_id:
                max_id = row_id
        except:
            pass
    
    print(f"Max original ID: {max_id}")
    print(f"Scaling to {multiplier}x...")
    
    scaled_rows = []
    
    for copy_num in range(multiplier):
        id_offset = max_id * copy_num
        
        for row in original_rows:
            new_row = row.copy()
            
            
            try:
                original_id = row.get('id', '').strip()
                if original_id.isdigit():
                    new_id = str(int(original_id) + id_offset)
                    new_row['id'] = new_id
                    
                    
                    listing_url = row.get('listing_url', '')
                    if listing_url and f'/rooms/{original_id}' in listing_url:
                        new_row['listing_url'] = listing_url.replace(
                            f'/rooms/{original_id}', 
                            f'/rooms/{new_id}'
                        )
            except:
                pass
            
            scaled_rows.append(new_row)
    
    print(f"Scaled records: {len(scaled_rows)}")
    print(f"Writing to {output_file}...")
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(scaled_rows)
    
    
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Output file size: {file_size:.1f} MB")
    print(f"Done!")
    print()
    
    return len(scaled_rows), file_size

def main():
    parser = argparse.ArgumentParser(description='Scale CSV data for performance testing')
    parser.add_argument('input_file', help='Input CSV file (e.g., listings.csv)')
    parser.add_argument('--multiplier', '-m', type=int, help='Scale multiplier (e.g., 2, 4, 6)')
    parser.add_argument('--output', '-o', help='Output file name')
    parser.add_argument('--all', action='store_true', help='Generate all scale levels (2x, 4x, 6x)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found")
        sys.exit(1)
    
    if args.all:
        
        base_name = os.path.splitext(args.input_file)[0]
        
        results = []
        for mult in [2, 4, 6]:
            output_file = f"{base_name}_{mult}x.csv"
            records, size = scale_csv_data(args.input_file, output_file, mult)
            results.append((mult, records, size, output_file))
        
        
        print("=" * 60)
        print("Summary:")
        print("=" * 60)
        print(f"{'Multiplier':<12} {'Records':<12} {'Size':<12} {'File'}")
        print("-" * 60)
        
        
        orig_size = os.path.getsize(args.input_file) / (1024 * 1024)
        with open(args.input_file, 'r', encoding='utf-8', errors='ignore') as f:
            orig_count = sum(1 for _ in f) - 1
        print(f"{'1x (orig)':<12} {orig_count:<12} {orig_size:.1f} MB{'':<6} {args.input_file}")
        
        for mult, records, size, filename in results:
            print(f"{f'{mult}x':<12} {records:<12} {f'{size:.1f} MB':<12} {filename}")
        
        print("=" * 60)
        print("\nNext steps:")
        print("1. Upload these files to S3:")
        for mult, _, _, filename in results:
            print(f"   aws s3 cp {filename} s3://airbnb-raw-data-han/")
        
    elif args.multiplier:
        
        if args.output:
            output_file = args.output
        else:
            base_name = os.path.splitext(args.input_file)[0]
            output_file = f"{base_name}_{args.multiplier}x.csv"
        
        scale_csv_data(args.input_file, output_file, args.multiplier)
    
    else:
        print("Error: Please specify --multiplier or --all")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()


