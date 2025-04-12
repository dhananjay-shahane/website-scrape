# Google Maps Business Scraper & Email Extractor

This repository contains two powerful Node.js scripts for extracting business information from Google Maps and then finding contact email addresses for those businesses.

## Author
Created by Dhananjay Shahane

## Overview

1. **Google Maps Scraper**: Scrapes business details (name, category, address, website, phone) from Google Maps for specified locations and search queries.
2. **Email Extractor**: Takes the CSV output from the Google Maps Scraper and finds email addresses from the business websites.

## Requirements

- Node.js 14+ 
- NPM
- The following npm packages:
  - playwright
  - csv-parser
  - csv-writer
  - fs (built-in)
  - path (built-in)
  - os (built-in)
  - worker_threads (built-in)

## Installation

1. Clone this repository
2. Install dependencies:

```bash
npm install playwright csv-parser csv-writer
```

3. For Playwright, you'll need to install browsers:

```bash
npx playwright install chromium
```

## Script 1: Google Maps Scraper

This script scrapes business information from Google Maps for specific search queries and locations.

### Features

- Multi-location support
- Multiple search queries per location to overcome limits
- Parallel processing with batching
- Retry mechanisms for reliability
- Detailed logging and progress monitoring
- Avoids duplicates across queries
- CSV output with business details

### Configuration

Edit these variables at the top of the script:

```javascript
const baseNameSheet = 'home_dec';           // Base name for output files
const batchSize = 10;                       // Number of businesses to process in parallel
const maxBusinessesToScrape = 0;            // Set to 0 for no limit

// Define locations and search queries
const locations = [
    {
        name: 'toledo_spain',
        queries: [
            'https://www.google.com/maps/search/Furniture+stores+in+toledo+spain',
            'https://www.google.com/maps/search/Interior+design+stores+in+toledo+spain'
        ]
    }
    // Add more locations as needed
];
```

### Usage

Run the script:

```bash
node google-maps-scraper.js
```

### Output

The script will create a `data` directory with two types of CSV files:
- Location-specific CSVs with columns: Name, Category, Address, Website, Phone, URL
- Combined CSV for all locations with columns: Location, Name, Category, Address, Website, Phone, URL

Example output filenames:
- `home_dec_toledo_spain_2025-04-09_19-46.csv`
- `home_dec_all_locations_2025-04-09_19-46.csv`

## Script 2: Email Extractor

This script takes the output CSV from the Google Maps Scraper and extracts email addresses from business websites.

### Features

- Multi-threaded processing using worker threads
- Smart email ranking algorithm
- Caching to avoid re-scraping
- Contact page detection
- Email filtering to avoid common non-personal emails
- CSV output with original data plus email column

### Configuration

Key settings at the top of the script:

```javascript
const config = {
  dataDir: path.join(__dirname, 'data'),            // Input/output directory
  inputFilename: 'home_dec_toledo_spain_2025-04-09_19-46.csv',  // Input file from scraper
  outputFilename: 'home_dec_toledo_spain_2025-04-09_19-46_emails.csv',  // Output file
  requestDelay: 300,                                // Delay between requests
  maxRetries: 3,                                    // Retries per website
  parallelScrapers: Math.max(1, Math.min(os.cpus().length - 1, 8)),  // Number of parallel threads
  maxEmailsPerSite: 3,                              // Limit emails per business
  removeRowsWithoutEmails: true,                    // Filter out rows without emails
  useWorkerThreads: true,                           // Use multi-threading
  cacheResults: true                                // Cache results
};
```

### Command Line Arguments

You can override configuration settings with command line arguments:

- `--limit N`: Process only the first N websites
- `--max-emails N`: Maximum emails to extract per site
- `--input filename.csv`: Input CSV file
- `--output filename.csv`: Output CSV file
- `--parallel N`: Number of parallel workers
- `--no-limit`: Don't limit the number of emails per site
- `--keep-empty`: Keep rows without emails in the output
- `--no-workers`: Disable worker threads
- `--no-cache`: Disable result caching

### Usage

Run the script:

```bash
node email-extractor.js
```

Or with options:

```bash
node email-extractor.js --limit 50 --max-emails 5 --input my-businesses.csv --output with-emails.csv
```

### Output

The script adds an "Emails" column to the original CSV data, containing found email addresses separated by commas.

## Performance Considerations

- These scripts are resource-intensive and may require adjustment based on your system capabilities
- For larger datasets, consider increasing the batch size and parallel workers
- Be aware of potential rate limiting from Google Maps and websites
- Adjust timeout values if you're experiencing connection issues

## Ethical and Legal Considerations

- Always respect website terms of service and robots.txt
- Consider website server load when scraping
- Be mindful of local privacy laws regarding email collection
- This tool is intended for legitimate business research only

## Troubleshooting

- If the Google Maps Scraper fails, check if Google Maps has changed its DOM structure
- If websites aren't loading, try increasing navigation timeout
- If you're getting blocked, adjust request delays and consider using proxies
- Check debug screenshots in the data directory for visual troubleshooting

## License and Usage Warning

**IMPORTANT**: This project is provided as-is with no warranty. This code is for personal, educational, and non-commercial purposes only. 

**DO NOT USE THIS PROJECT FOR COMMERCIAL PURPOSES** without obtaining proper permission from website owners and ensuring compliance with:
1. Google Maps Terms of Service
2. Individual website Terms of Service
3. Local and international data protection laws (GDPR, CCPA, etc.)
4. Anti-spam laws and regulations

Unauthorized scraping of websites and Google Maps may violate terms of service and could potentially result in legal consequences. The author assumes no responsibility for any misuse of this code or any violations that may result from its use.

Copyright © 2025 Dhananjay Shahane. All rights reserved.
