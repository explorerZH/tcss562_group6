import json
import logging
import os
import subprocess
import re
import uuid
import time
import boto3
import csv
from io import StringIO
from datetime import datetime

def runCommand(command):
    return os.popen(command).read()

# Global variables
invocations = 0
initialization_time = int(round(time.time() * 1000))
try:
    ticks_per_second = int(runCommand("getconf CLK_TCK"))
except:
    ticks_per_second = 100

class Inspector:
    def __init__(self):
        global invocations
        global initialization_time
        invocations += 1
        
        self.__startTime = int(round(time.time() * 1000))
        self.__attributes = {
            "version": 0.7, 
            "lang": "python", 
            "startTime": self.__startTime,
            "invocations": invocations,
            "initializationTime": initialization_time
        }
        self.__cpuPolls = []
        self.__inspectedCPU = False
        self.__inspectedMemory = False
        self.__inspectedContainer = False
        self.__inspectedPlatform = False

    def inspectContainer(self):
        self.__inspectedContainer = True
        myUuid = ''
        newContainer = 1
        if os.path.isfile('/tmp/container-id'):
            with open('/tmp/container-id', 'r') as f:
                myUuid = f.readline()
            newContainer = 0
        else:
            with open('/tmp/container-id', 'w') as f:
                myUuid = str(uuid.uuid4())
                f.write(myUuid)
        self.__attributes['uuid'] = myUuid
        self.__attributes['newcontainer'] = newContainer

    def inspectCPUInfo(self):
        try:
            with open('/proc/cpuinfo', 'r') as file:
                cpuInfo = file.read()
            lines = cpuInfo.split('\n')
            cpu_count = 0
            cpu_info = {}
            for line in lines:
                try:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        key = key.strip().replace(" ", "_")
                        value = value.strip()
                        if key == 'processor':
                            cpu_count += 1
                        elif key == 'model_name':
                            self.__attributes['cpuType'] = value
                        elif key == 'model':
                            self.__attributes['cpuModel'] = value
                except:
                    pass
            self.__attributes['cpuCores'] = cpu_count
        
            import platform
            arch = platform.machine()
            if arch == 'aarch64':
                self.__attributes['architecture'] = 'arm64'
            else:
                self.__attributes['architecture'] = 'x86_64'
        except Exception as e:
            self.__attributes['cpuInfoError'] = str(e)

    def pollCPUStats(self):
        global ticks_per_second
        timeStamp = int(round(time.time() * 1000))
        cpuValues = ["cpuUser", "cpuNice", "cpuKernel", "cpuIdle", "cpuIOWait", "cpuIrq", "cpuSoftIrq", "cpuSteal"]
        data = {"time": timeStamp}
        tick_rate = 1000 / ticks_per_second
        try:
            with open('/proc/stat', 'r') as file:
                stats = file.read()
            lines = stats.split('\n')
            for line in lines:
                if line.startswith('cpu '):
                    values = line.split()
                    stats = {}
                    for i, metric in enumerate(cpuValues):
                        if i + 1 < len(values):
                            stats[metric] = int(values[i + 1]) * tick_rate
                    data['cpuTotal'] = stats
                elif line.startswith('btime'):
                    data['btime'] = int(line.split()[1])
        except:
            pass
        self.__cpuPolls.append(data)

    def inspectCPU(self):
        self.__inspectedCPU = True
        self.pollCPUStats()
        if self.__cpuPolls and 'cpuTotal' in self.__cpuPolls[0]:
            for metric, value in self.__cpuPolls[0]['cpuTotal'].items():
                self.__attributes[metric] = value
        if self.__cpuPolls and 'btime' in self.__cpuPolls[0]:
            self.__attributes['bootTime'] = self.__cpuPolls[0]['btime']

    def inspectCPUDelta(self):
        if self.__inspectedCPU:
            self.pollCPUStats()
            if len(self.__cpuPolls) >= 2:
                for metric in self.__cpuPolls[0].get('cpuTotal', {}).keys():
                    start_val = self.__cpuPolls[0]['cpuTotal'].get(metric, 0)
                    end_val = self.__cpuPolls[-1]['cpuTotal'].get(metric, 0)
                    self.__attributes[metric + "Delta"] = end_val - start_val

    def inspectMemory(self):
        self.__inspectedMemory = True
        try:
            with open('/proc/meminfo', 'r') as file:
                for line in file:
                    if line.startswith('MemTotal:'):
                        self.__attributes['totalMemory'] = int(line.split()[1])
                    elif line.startswith('MemFree:'):
                        self.__attributes['freeMemory'] = int(line.split()[1])
        except:
            pass
        try:
            with open('/proc/vmstat', 'r') as file:
                for line in file:
                    if line.startswith('pgfault'):
                        self.__attributes['pageFaults'] = int(line.split()[1])
                    elif line.startswith('pgmajfault'):
                        self.__attributes['majorPageFaults'] = int(line.split()[1])
        except:
            pass

    def inspectMemoryDelta(self):
        if self.__inspectedMemory:
            try:
                with open('/proc/vmstat', 'r') as file:
                    for line in file:
                        if line.startswith('pgfault'):
                            self.__attributes['pageFaultsDelta'] = int(line.split()[1]) - self.__attributes.get('pageFaults', 0)
                        elif line.startswith('pgmajfault'):
                            self.__attributes['majorPageFaultsDelta'] = int(line.split()[1]) - self.__attributes.get('majorPageFaults', 0)
            except:
                pass

    def inspectPlatform(self):
        self.__inspectedPlatform = True
        if os.environ.get('AWS_LAMBDA_LOG_STREAM_NAME'):
            self.__attributes['platform'] = "AWS Lambda"
            self.__attributes['containerID'] = os.environ.get('AWS_LAMBDA_LOG_STREAM_NAME', '')
            self.__attributes['functionName'] = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', '')
            self.__attributes['functionMemory'] = os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', '')
            self.__attributes['functionRegion'] = os.environ.get('AWS_REGION', '')
        else:
            self.__attributes['platform'] = "Unknown"

    def inspectLinux(self):
        try:
            self.__attributes['linuxVersion'] = runCommand('uname -a').strip()
        except:
            pass

    def inspectAll(self):
        self.inspectContainer()
        self.inspectCPUInfo()
        self.inspectPlatform()
        self.inspectLinux()
        self.inspectMemory()
        self.inspectCPU()
        self.addTimeStamp("frameworkRuntime")

    def inspectAllDeltas(self):
        if 'frameworkRuntime' in self.__attributes:
            self.addTimeStamp("userRuntime", self.__startTime + self.__attributes['frameworkRuntime'])
        deltaTime = int(round(time.time() * 1000))
        self.inspectCPUDelta()
        self.inspectMemoryDelta()
        self.addTimeStamp("frameworkRuntimeDeltas", deltaTime)

    def addAttribute(self, key, value):
        self.__attributes[key] = value

    def getAttribute(self, key):
        return self.__attributes.get(key)

    def addTimeStamp(self, key, timeSince=None):
        if timeSince is None:
            timeSince = self.__startTime
        currentTime = int(round(time.time() * 1000))
        self.__attributes[key] = currentTime - timeSince

    def finish(self):
        self.addTimeStamp('runtime')
        self.__attributes['endTime'] = int(round(time.time() * 1000))
        return self.__attributes


# ============================================================================
# Helper Functions for Data Cleaning
# ============================================================================

def clean_text(text, max_length=None):
    if not text:
        return ''
    text = re.sub(r'[\r\n\t]+', ' ', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    if max_length and len(text) > max_length:
        text = text[:max_length-3] + '...'
    return text

def clean_price(price_str):
    if not price_str:
        return 0.0
    try:
        cleaned = re.sub(r'[^\d.]', '', str(price_str))
        return round(float(cleaned), 2) if cleaned else 0.0
    except:
        return 0.0

def clean_float(value):
    if not value or value == 'N/A':
        return 0.0
    try:
        return float(value)
    except:
        return 0.0

def clean_int(value):
    if not value or value == 'N/A':
        return 0
    try:
        return int(float(value))
    except:
        return 0

def clean_percentage(percent_str):
    if not percent_str or percent_str == 'N/A':
        return 0.0
    try:
        return float(str(percent_str).replace('%', '').strip())
    except:
        return 0.0

def convert_boolean(value):
    return 1 if str(value).lower() in ['t', 'true', '1', 'yes'] else 0

def clean_date(date_str):
    if not date_str:
        return ''
    for fmt in ['%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y']:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except:
            continue
    return date_str

def clean_amenities(amenities_str):
    if not amenities_str:
        return 0
    try:
        cleaned = amenities_str.strip('{}')
        return len([i.strip() for i in cleaned.split(',')]) if cleaned else 0
    except:
        return 0

def simplify_room_type(room_type):
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

def categorize_price(price):
    if price <= 0: return 'unknown'
    elif price < 75: return 'budget'
    elif price < 150: return 'moderate'
    elif price < 300: return 'expensive'
    else: return 'luxury'

def categorize_reviews(count):
    if count == 0: return 'no_reviews'
    elif count < 5: return 'few'
    elif count < 20: return 'moderate'
    elif count < 50: return 'many'
    else: return 'very_popular'

def categorize_host(is_superhost, response_rate):
    if is_superhost: return 'superhost'
    elif response_rate >= 90: return 'responsive'
    elif response_rate >= 50: return 'moderate'
    else: return 'low_response'

def categorize_availability(days):
    if days == 0: return 'not_available'
    elif days < 30: return 'rarely_available'
    elif days < 180: return 'occasionally_available'
    elif days < 300: return 'mostly_available'
    else: return 'highly_available'


# ============================================================================
# Main Lambda Handler
# ============================================================================

def lambda_handler(event, context):
    """
    Transform Lambda with SAAF instrumentation for architecture comparison
    """
    
    inspector = Inspector()
    inspector.inspectAll()
    inspector.addTimeStamp("saaf_init_complete")
    
    SOURCE_BUCKET = 'airbnb-raw-data-han'
    SOURCE_KEY = 'listings.csv'
    DEST_BUCKET = 'airbnb-clean-data-han'
    
    s3 = boto3.client('s3')
    
    arch_type = inspector.getAttribute('architecture') or 'unknown'
    inspector.addAttribute("arch_type", arch_type)
    
    try:
        inspector.addTimeStamp("start_read")
        
        response = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
        raw_data = response['Body'].read().decode('utf-8', errors='ignore')
        
        reader = csv.DictReader(StringIO(raw_data))
        raw_rows = list(reader)
        
        inspector.addAttribute("raw_count", len(raw_rows))
        inspector.addTimeStamp("end_read")
        
        inspector.addTimeStamp("start_transform")
        
        cleaned_rows = []
        error_count = 0
        
        for idx, row in enumerate(raw_rows):
            try:
                if not row.get('id', '').strip() and not row.get('name', '').strip():
                    continue
                
                price = clean_price(row.get('price', '0'))
                if price <= 0 or price > 10000:
                    continue
                if not row.get('id', '').strip():
                    continue
                
                cleaned_record = {
                    'id': row.get('id', '').strip(),
                    'listing_url': row.get('listing_url', ''),
                    'last_scraped': row.get('last_scraped', ''),
                    'name': clean_text(row.get('name', ''), max_length=200),
                    'description': clean_text(row.get('description', ''), max_length=1000),
                    'property_type': row.get('property_type', 'Unknown'),
                    'room_type': row.get('room_type', 'Unknown'),
                    'host_id': row.get('host_id', '').strip(),
                    'host_name': clean_text(row.get('host_name', ''), max_length=100),
                    'host_since': clean_date(row.get('host_since', '')),
                    'host_response_time': row.get('host_response_time', 'N/A'),
                    'host_response_rate': clean_percentage(row.get('host_response_rate', '')),
                    'host_acceptance_rate': clean_percentage(row.get('host_acceptance_rate', '')),
                    'host_is_superhost': convert_boolean(row.get('host_is_superhost', 'f')),
                    'host_listings_count': clean_int(row.get('host_listings_count', '0')),
                    'host_identity_verified': convert_boolean(row.get('host_identity_verified', 'f')),
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
                    'accommodates': clean_int(row.get('accommodates', '0')),
                    'bathrooms': clean_float(row.get('bathrooms', '0')),
                    'bedrooms': clean_int(row.get('bedrooms', '0')),
                    'beds': clean_int(row.get('beds', '0')),
                    'bed_type': row.get('bed_type', 'Unknown'),
                    'amenities': clean_amenities(row.get('amenities', '{}')),
                    'square_feet': clean_int(row.get('square_feet', '0')),
                    'price': price,
                    'weekly_price': clean_price(row.get('weekly_price', '0')),
                    'monthly_price': clean_price(row.get('monthly_price', '0')),
                    'security_deposit': clean_price(row.get('security_deposit', '0')),
                    'cleaning_fee': clean_price(row.get('cleaning_fee', '0')),
                    'guests_included': clean_int(row.get('guests_included', '1')),
                    'extra_people': clean_price(row.get('extra_people', '0')),
                    'minimum_nights': clean_int(row.get('minimum_nights', '1')),
                    'maximum_nights': clean_int(row.get('maximum_nights', '365')),
                    'instant_bookable': convert_boolean(row.get('instant_bookable', 'f')),
                    'cancellation_policy': row.get('cancellation_policy', 'flexible'),
                    'has_availability': convert_boolean(row.get('has_availability', 't')),
                    'availability_30': clean_int(row.get('availability_30', '0')),
                    'availability_60': clean_int(row.get('availability_60', '0')),
                    'availability_90': clean_int(row.get('availability_90', '0')),
                    'availability_365': clean_int(row.get('availability_365', '0')),
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
                continue
        
        inspector.addAttribute("clean_count", len(cleaned_rows))
        inspector.addAttribute("removed_count", len(raw_rows) - len(cleaned_rows))
        inspector.addAttribute("error_count", error_count)
        inspector.addTimeStamp("end_transform")
        

        if cleaned_rows:
            inspector.addTimeStamp("start_save")
            
           
            cleaned_rows.sort(key=lambda x: int(x['id']) if x['id'].isdigit() else 0)
            for i, row in enumerate(cleaned_rows, start=1):
                row['sequential_id'] = i
            
         
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_key = f'clean_listings_{arch_type}_{timestamp}.csv'
            
        
            fieldnames = ['sequential_id'] + [k for k in cleaned_rows[0].keys() if k != 'sequential_id']
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)
            csv_content = output.getvalue()
            
        
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=output_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            inspector.addAttribute("output_file", f's3://{DEST_BUCKET}/{output_key}')
            inspector.addAttribute("file_size_bytes", len(csv_content))
            inspector.addAttribute("records_processed", len(cleaned_rows))
            inspector.addTimeStamp("end_save")
            
        
            inspector.inspectAllDeltas()
            
            return inspector.finish()
        
        else:
            inspector.inspectAllDeltas()
            inspector.addAttribute("error", "No valid records after cleaning")
            return inspector.finish()
            
    except Exception as e:
        inspector.inspectAllDeltas()
        inspector.addAttribute("error", str(e))
        import traceback
        inspector.addAttribute("traceback", traceback.format_exc())
        return inspector.finish()