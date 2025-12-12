import json
import boto3
import csv
from io import StringIO
from datetime import datetime
import re
import base64

def lambda_handler(event, context):
    """
    Transform Lambda: Clean and process Airbnb listings data
    Handles complex CSV with embedded commas and special characters
    """
    
    SOURCE_BUCKET = 'airbnb-raw-data-han' 
    SOURCE_KEY = 'listings.csv'
    DEST_BUCKET = 'airbnb-clean-data-han'  
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Performance tracking
    start_time = datetime.now()
    metrics = {
        'start_time': start_time.isoformat(),
        'source_file': f's3://{SOURCE_BUCKET}/{SOURCE_KEY}'
    }
    
    try:
        # ========== 1. READ RAW DATA ==========
        print(f"Reading data from: s3://{SOURCE_BUCKET}/{SOURCE_KEY}")
        
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
        raw_data = response['Body'].read().decode('utf-8', errors='ignore')
        
        # Parse CSV with proper handling of complex fields
        reader = csv.DictReader(StringIO(raw_data))
        raw_rows = list(reader)
        metrics['raw_count'] = len(raw_rows)
        print(f"Loaded {len(raw_rows)} raw records")
        
        # ========== 2. DATA CLEANING AND TRANSFORMATION ==========
        print("Starting data transformation...")
        cleaned_rows = []
        error_count = 0
        
        for idx, row in enumerate(raw_rows):
            try:
                # Skip empty rows (common at end of file)
                if not row.get('id', '').strip() and not row.get('name', '').strip():
                    continue
                
                # Extract and clean core fields
                price = clean_price(row.get('price', '0'))
                
                # Skip invalid records
                if price <= 0 or price > 10000:
                    continue
                    
                if not row.get('id', '').strip():
                    continue
                
                # Build cleaned record with essential fields
                cleaned_record = {
                    # === Core Identifiers ===
                    'id': row.get('id', '').strip(),
                    'listing_url': row.get('listing_url', ''),
                    'last_scraped': row.get('last_scraped', ''),
                    
                    # === Property Details ===
                    'name': clean_text(row.get('name', ''), max_length=200),
                    'description': clean_text(row.get('description', ''), max_length=1000),
                    'property_type': row.get('property_type', 'Unknown'),
                    'room_type': row.get('room_type', 'Unknown'),
                    
                    # === Host Information ===
                    'host_id': row.get('host_id', '').strip(),
                    'host_name': clean_text(row.get('host_name', ''), max_length=100),
                    'host_since': clean_date(row.get('host_since', '')),
                    'host_response_time': row.get('host_response_time', 'N/A'),
                    'host_response_rate': clean_percentage(row.get('host_response_rate', '')),
                    'host_acceptance_rate': clean_percentage(row.get('host_acceptance_rate', '')),
                    'host_is_superhost': convert_boolean(row.get('host_is_superhost', 'f')),
                    'host_listings_count': clean_int(row.get('host_listings_count', '0')),
                    'host_identity_verified': convert_boolean(row.get('host_identity_verified', 'f')),
                    
                    # === Location ===
                    'street': clean_text(row.get('street', ''), max_length=200),
                    'neighbourhood': row.get('neighbourhood', '').strip(),
                    'neighbourhood_cleansed': row.get('neighbourhood_cleansed', '').strip(),
                    'neighbourhood_group_cleansed': row.get('neighbourhood_group_cleansed', '').strip(),
                    'city': row.get('city', 'Seattle').strip(),
                    'state': row.get('state', 'WA').strip(),
                    'zipcode': row.get('zipcode', '').strip(),
                    'latitude': clean_float(row.get('latitude', '0')),
                    'longitude': clean_float(row.get('longitude', '0')),
                    'is_location_exact': convert_boolean(row.get('is_location_exact', 'f')),
                    
                    # === Capacity and Amenities ===
                    'accommodates': clean_int(row.get('accommodates', '0')),
                    'bathrooms': clean_float(row.get('bathrooms', '0')),
                    'bedrooms': clean_int(row.get('bedrooms', '0')),
                    'beds': clean_int(row.get('beds', '0')),
                    'bed_type': row.get('bed_type', 'Unknown'),
                    'amenities': clean_amenities(row.get('amenities', '{}')),
                    'square_feet': clean_int(row.get('square_feet', '0')),
                    
                    # === Pricing ===
                    'price': price,
                    'weekly_price': clean_price(row.get('weekly_price', '0')),
                    'monthly_price': clean_price(row.get('monthly_price', '0')),
                    'security_deposit': clean_price(row.get('security_deposit', '0')),
                    'cleaning_fee': clean_price(row.get('cleaning_fee', '0')),
                    'guests_included': clean_int(row.get('guests_included', '1')),
                    'extra_people': clean_price(row.get('extra_people', '0')),
                    
                    # === Booking Rules ===
                    'minimum_nights': clean_int(row.get('minimum_nights', '1')),
                    'maximum_nights': clean_int(row.get('maximum_nights', '365')),
                    'instant_bookable': convert_boolean(row.get('instant_bookable', 'f')),
                    'cancellation_policy': row.get('cancellation_policy', 'flexible'),
                    
                    # === Availability ===
                    'has_availability': convert_boolean(row.get('has_availability', 't')),
                    'availability_30': clean_int(row.get('availability_30', '0')),
                    'availability_60': clean_int(row.get('availability_60', '0')),
                    'availability_90': clean_int(row.get('availability_90', '0')),
                    'availability_365': clean_int(row.get('availability_365', '0')),
                    
                    # === Reviews ===
                    'number_of_reviews': clean_int(row.get('number_of_reviews', '0')),
                    'first_review': clean_date(row.get('first_review', '')),
                    'last_review': clean_date(row.get('last_review', '')),
                    'review_scores_rating': clean_float(row.get('review_scores_rating', '0')),
                    'review_scores_accuracy': clean_float(row.get('review_scores_accuracy', '0')),
                    'review_scores_cleanliness': clean_float(row.get('review_scores_cleanliness', '0')),
                    'review_scores_checkin': clean_float(row.get('review_scores_checkin', '0')),
                    'review_scores_communication': clean_float(row.get('review_scores_communication', '0')),
                    'review_scores_location': clean_float(row.get('review_scores_location', '0')),
                    'review_scores_value': clean_float(row.get('review_scores_value', '0')),
                    'reviews_per_month': clean_float(row.get('reviews_per_month', '0')),
                    
                    # === FEATURE ENGINEERING - New Calculated Fields ===
                    'price_category': categorize_price(price),
                    'review_category': categorize_reviews(clean_int(row.get('number_of_reviews', '0'))),
                    'host_category': categorize_host(
                        convert_boolean(row.get('host_is_superhost', 'f')),
                        clean_percentage(row.get('host_response_rate', ''))
                    ),
                    'availability_category': categorize_availability(clean_int(row.get('availability_365', '0'))),
                    'room_type_simplified': simplify_room_type(row.get('room_type', '')),
                    'is_professional_host': 1 if clean_int(row.get('host_listings_count', '0')) > 3 else 0,
                    'has_cleaning_fee': 1 if clean_price(row.get('cleaning_fee', '0')) > 0 else 0,
                    'price_per_guest': round(price / max(clean_int(row.get('accommodates', '1')), 1), 2),
                }
                
                cleaned_rows.append(cleaned_record)
                
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Only log first 5 errors
                    print(f"Error processing row {idx}: {str(e)}")
                continue
        
        metrics['clean_count'] = len(cleaned_rows)
        metrics['removed_count'] = metrics['raw_count'] - metrics['clean_count']
        metrics['error_count'] = error_count
        print(f"Transformation complete: {len(cleaned_rows)} clean records from {metrics['raw_count']} raw records")
        print(f"Removed {metrics['removed_count']} invalid records, encountered {error_count} errors")
        
        # ========== 3. SAVE CLEANED DATA ==========
        if cleaned_rows:
            # Generate output filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_key = f'clean_listings_{timestamp}.csv'
            
            # Convert to CSV
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=cleaned_rows[0].keys())
            writer.writeheader()
            writer.writerows(cleaned_rows)
            csv_content = output.getvalue()
            
            # Upload to S3
            print(f"Uploading to: s3://{DEST_BUCKET}/{output_key}")
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=output_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            # Generate presigned URL for team collaboration
            download_url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': DEST_BUCKET, 'Key': output_key},
                ExpiresIn=604800  # 7 days
            )
            
            # ========== 4. CALCULATE METRICS ==========
            end_time = datetime.now()
            metrics['processing_time_seconds'] = (end_time - start_time).total_seconds()
            metrics['output_file'] = f's3://{DEST_BUCKET}/{output_key}'
            metrics['download_url'] = download_url
            metrics['file_size_mb'] = len(csv_content) / (1024 * 1024)
            
            # Data quality metrics
            avg_review_score = sum(r['review_scores_rating'] for r in cleaned_rows) / len(cleaned_rows)
            metrics['data_quality'] = {
                'avg_price': round(sum(r['price'] for r in cleaned_rows) / len(cleaned_rows), 2),
                'avg_review_score': round(avg_review_score, 2),
                'superhosts_count': sum(1 for r in cleaned_rows if r['host_is_superhost']),
                'instant_bookable_count': sum(1 for r in cleaned_rows if r['instant_bookable'])
            }
            
            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Transform completed successfully',
                    'output_location': f's3://{DEST_BUCKET}/{output_key}',
                    'download_url': download_url,
                    'metrics': metrics,
                    'team_instructions': {
                        'for_load_stage': f"Download from: {download_url}",
                        'for_query_stage': 'Data includes 70 cleaned fields + 8 engineered features'
                    }
                }, indent=2)
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No valid records after cleaning'})
            }
            
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'metrics': metrics
            })
        }

# ============ HELPER FUNCTIONS ============

def clean_text(text, max_length=None):
    """Clean text fields: remove newlines, excess spaces, etc."""
    if not text:
        return ''
    # Remove control characters and normalize whitespace
    text = re.sub(r'[\r\n\t]+', ' ', str(text))
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    if max_length and len(text) > max_length:
        text = text[:max_length-3] + '...'
    return text

def clean_price(price_str):
    """Clean price: $1,234.56 -> 1234.56"""
    if not price_str:
        return 0.0
    try:
        # Remove currency symbols and thousands separators
        cleaned = re.sub(r'[^\d.]', '', str(price_str))
        if cleaned:
            return round(float(cleaned), 2)
        return 0.0
    except:
        return 0.0

def clean_float(value):
    """Safely convert to float"""
    if not value or value == 'N/A':
        return 0.0
    try:
        return float(value)
    except:
        return 0.0

def clean_int(value):
    """Safely convert to integer"""
    if not value or value == 'N/A':
        return 0
    try:
        # Handle floats like "2.0"
        return int(float(value))
    except:
        return 0

def clean_percentage(percent_str):
    """Clean percentage: 85% -> 85.0"""
    if not percent_str or percent_str == 'N/A':
        return 0.0
    try:
        cleaned = str(percent_str).replace('%', '').strip()
        return float(cleaned)
    except:
        return 0.0

def convert_boolean(value):
    """Convert t/f to 1/0"""
    if str(value).lower() in ['t', 'true', '1', 'yes']:
        return 1
    return 0

def clean_date(date_str):
    """Standardize date format"""
    if not date_str:
        return ''
    try:
        # Try to parse common date formats
        for fmt in ['%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y']:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except:
                continue
        return date_str  # Return original if can't parse
    except:
        return ''

def clean_amenities(amenities_str):
    """Extract amenities count from the string"""
    if not amenities_str:
        return 0
    try:
        # Remove curly braces and count items
        cleaned = amenities_str.strip('{}')
        if cleaned:
            items = [item.strip() for item in cleaned.split(',')]
            return len(items)
        return 0
    except:
        return 0

def simplify_room_type(room_type):
    """Simplify room type to standard categories"""
    if not room_type:
        return 'Other'
    room_lower = room_type.lower()
    if 'entire' in room_lower or 'apt' in room_lower:
        return 'Entire'
    elif 'private' in room_lower:
        return 'Private'
    elif 'shared' in room_lower:
        return 'Shared'
    return 'Other'

# ============ FEATURE ENGINEERING FUNCTIONS ============

def categorize_price(price):
    """Categorize price into buckets"""
    if price <= 0:
        return 'unknown'
    elif price < 75:
        return 'budget'
    elif price < 150:
        return 'moderate'
    elif price < 300:
        return 'expensive'
    else:
        return 'luxury'

def categorize_reviews(count):
    """Categorize review count"""
    if count == 0:
        return 'no_reviews'
    elif count < 5:
        return 'few'
    elif count < 20:
        return 'moderate'
    elif count < 50:
        return 'many'
    else:
        return 'very_popular'

def categorize_host(is_superhost, response_rate):
    """Categorize host quality"""
    if is_superhost:
        return 'superhost'
    elif response_rate >= 90:
        return 'responsive'
    elif response_rate >= 50:
        return 'moderate'
    else:
        return 'low_response'

def categorize_availability(days):
    """Categorize availability"""
    if days == 0:
        return 'not_available'
    elif days < 30:
        return 'rarely_available'
    elif days < 180:
        return 'occasionally_available'
    elif days < 300:
        return 'mostly_available'
    else:
        return 'highly_available'