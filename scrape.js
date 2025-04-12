const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

(async () => {
    // Configuration - Increased batch size and removed business limit
    const baseNameSheet = 'home_dec';
    const batchSize = 10; // Increased for faster processing, adjust based on your system's capability
    const maxBusinessesToScrape = 0; // Set to 0 for no limit (will try to get all available)
    
    // Multiple locations configuration with search variants to overcome limits
    const locations = [
        // Toledo Spain with multiple search queries to overcome limits
        {
            name: 'toledo_spain',
            queries: [
                'https://www.google.com/maps/search/Furniture+stores+in+toledo+spain',
                'https://www.google.com/maps/search/Interior+design+stores+in+toledo+spain'
            ]
        }
        // Add more locations as needed
    ];
    
    // Create data directory if it doesn't exist
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true });
    }
    
    // Generate timestamp for filename
    const now = new Date();
    const timestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}-${String(now.getMinutes()).padStart(2, '0')}`;
    
    console.time("Total Execution Time");
    console.log(`Starting Enhanced Google Maps Scraper at ${now.toLocaleString()}`);
    console.log(`Configuration: Locations = ${locations.length}, Batch size = ${batchSize}, Max businesses per location = ${maxBusinessesToScrape > 0 ? maxBusinessesToScrape : 'no limit'}`);
    
    // Create combined output file for all locations
    const combinedOutputFilename = path.join(dataDir, `${baseNameSheet}_all_locations_${timestamp}.csv`);
    const csvHeader = 'Location,Name,Category,Address,Website,Phone,Url\n';
    fs.writeFileSync(combinedOutputFilename, csvHeader);
    
    try {
        // Launch browser with more specific options to avoid crashes
        const browser = await chromium.launch({ 
            headless: true,
            args: [
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security', // May help with some restrictions
                '--disable-features=IsolateOrigins,site-per-process' // May help with some restrictions
            ]
        });
        
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            viewport: { width: 1920, height: 1080 },
            geolocation: { longitude: 2.3522, latitude: 48.8566 }, // Paris (neutral location)
            locale: 'en-US',
            timezoneId: 'Europe/Madrid'
        });
        
        // Add error logging
        context.on('console', msg => {
            if (msg.type() === 'error') {
                console.error(`Browser console error: ${msg.text()}`);
            }
        });
        
        // Process each location one by one
        for (let locationIndex = 0; locationIndex < locations.length; locationIndex++) {
            const location = locations[locationIndex];
            console.log(`\n[${locationIndex + 1}/${locations.length}] Processing location: ${location.name}`);
            console.time(`Location Execution Time: ${location.name}`);
            
            // Create location-specific output file
            const locationOutputFilename = path.join(dataDir, `${baseNameSheet}_${location.name}_${timestamp}.csv`);
            fs.writeFileSync(locationOutputFilename, 'Name,Category,Address,Website,Phone,Url\n');
            
            // Set to track unique business URLs to avoid duplicates across queries
            const uniqueUrlsSet = new Set();
            
            // Process each search query for this location
            for (let queryIndex = 0; queryIndex < location.queries.length; queryIndex++) {
                const searchUrl = location.queries[queryIndex];
                console.log(`\nProcessing query ${queryIndex + 1}/${location.queries.length} for ${location.name}`);
                console.log(`Search URL: ${searchUrl}`);
                
                const page = await context.newPage();
                
                try {
                    // Clear cookies and cache between queries to avoid personalization
                    await context.clearCookies();
                    
                    // Set longer timeout for initial navigation
                    console.log(`Navigating to Google Maps...`);
                    try {
                        await page.goto(searchUrl, { timeout: 90000, waitUntil: 'networkidle' });
                        console.log('Page loaded, waiting for content...');
                    } catch (err) {
                        console.error(`Error loading page: ${err.message}`);
                        console.log('Skipping this query and continuing with the next one.');
                        await page.close().catch(err => console.error(`Error closing page: ${err.message}`));
                        continue;
                    }
                    
                    // Handle potential consent dialogs
                    try {
                        const consentButton = await page.$('button:has-text("Accept all")');
                        if (consentButton) {
                            await consentButton.click();
                            console.log('Accepted consent dialog');
                            await page.waitForTimeout(1000);
                        }
                    } catch (err) {
                        console.log('No consent dialog detected or error handling it');
                    }
                    
                    // Wait for content with a more reliable selector
                    try {
                        // Trying different possible selectors for the Google Maps interface
                        await Promise.race([
                            page.waitForSelector('[jstcache="3"]', { timeout: 30000 }),
                            page.waitForSelector('div[role="main"]', { timeout: 30000 }),
                            page.waitForSelector('div[aria-label*="Results"]', { timeout: 30000 }),
                            page.waitForSelector('div.section-result-content', { timeout: 30000 })
                        ]);
                        console.log('Main content loaded successfully');
                    } catch (err) {
                        console.error(`Selector wait error: ${err.message}`);
                        
                        // Take a screenshot for debugging
                        await page.screenshot({ path: path.join(dataDir, `debug_screenshot_${location.name}_q${queryIndex}.png`) });
                        console.log(`Saved debug screenshot to debug_screenshot_${location.name}_q${queryIndex}.png`);
                        
                        console.log('Skipping this query and continuing with the next one.');
                        await page.close().catch(err => console.error(`Error closing page: ${err.message}`));
                        continue;
                    }
                    
                    // Add a brief delay to ensure page is fully loaded
                    await page.waitForTimeout(3000);
                    
                    console.log('Starting to scroll through results...');
                    
                    // Try different scroll container selectors (Google Maps UI can vary)
                    const scrollSelectors = [
                        'xpath=/html/body/div[2]/div[3]/div[8]/div[9]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]',
                        'div[role="feed"]',
                        'div[aria-label*="Results"] > div',
                        'div.section-scrollbox',
                        'div[jsaction*="scrollable"]'
                    ];
                    
                    let scrollable = null;
                    for (const selector of scrollSelectors) {
                        try {
                            const element = await page.$(selector);
                            if (element) {
                                scrollable = element;
                                console.log(`Found scrollable element with selector: ${selector}`);
                                break;
                            }
                        } catch (err) {
                            continue;
                        }
                    }
                    
                    // If no predefined selector works, try a broader approach
                    if (!scrollable) {
                        try {
                            // Look for any scrollable div that might contain results
                            const potentialScrollables = await page.$$('div[style*="overflow"]');
                            for (const element of potentialScrollables) {
                                const isScrollable = await element.evaluate(node => {
                                    return node.scrollHeight > node.clientHeight;
                                });
                                
                                if (isScrollable) {
                                    scrollable = element;
                                    console.log('Found scrollable element with style-based detection');
                                    break;
                                }
                            }
                        } catch (err) {
                            console.log('Failed to find scrollable element with style-based detection');
                        }
                    }
                    
                    if (!scrollable) {
                        console.error(`Could not find any scrollable element`);
                        
                        // Take a screenshot for debugging
                        await page.screenshot({ path: path.join(dataDir, `scrollable_debug_${location.name}_q${queryIndex}.png`) });
                        console.log(`Saved debug screenshot to scrollable_debug_${location.name}_q${queryIndex}.png`);
                        
                        // Try to extract without scrolling
                        console.log('Attempting to extract visible results without scrolling...');
                    }

                    // Aggressive scrolling to load all results
                    let previousHeight = 0;
                    let currentHeight = 0;
                    let noChangeCount = 0;
                    let scrollCount = 0;
                    const maxScrolls = 200; // Increased maximum scroll attempts
                    
                    if (scrollable) {
                        while (noChangeCount < 5 && scrollCount < maxScrolls) {
                            try {
                                previousHeight = await scrollable.evaluate(node => node.scrollHeight);
                                
                                // Mix of small and large scrolls to trigger different loading behaviors
                                if (scrollCount % 5 === 0) {
                                    await scrollable.evaluate(node => node.scrollBy(0, 1000)); // Larger scroll
                                } else {
                                    await scrollable.evaluate(node => node.scrollBy(0, 300)); // Smaller scroll
                                }
                                
                                // Random delay between 800-1200ms to appear more human-like
                                const randomDelay = 800 + Math.floor(Math.random() * 400);
                                await page.waitForTimeout(randomDelay);
                                
                                // Every few scrolls, try clicking "Show more results" if it exists
                                if (scrollCount % 10 === 7) {
                                    try {
                                        const showMoreButtons = await page.$$('button:has-text("Show more results"), span:has-text("Show more")');
                                        if (showMoreButtons.length > 0) {
                                            await showMoreButtons[0].click();
                                            console.log('Clicked "Show more results" button');
                                            await page.waitForTimeout(2000); // Wait for new results to load
                                        }
                                    } catch (err) {
                                        // Ignore errors, button might not exist
                                    }
                                }
                                
                                currentHeight = await scrollable.evaluate(node => node.scrollHeight);
                                
                                // Check if we've reached the end of results
                                const endOfResults = await page.evaluate(() => {
                                    return document.body.innerText.includes("You've reached the end of the list") || 
                                           document.body.innerText.includes("No more results") ||
                                           document.body.innerText.includes("End of results");
                                });
                                
                                scrollCount++;
                                
                                if (scrollCount % 10 === 0) {
                                    console.log(`Scrolled ${scrollCount} times, still loading results...`);
                                }
                                
                                // If no new content loaded after scroll
                                if (previousHeight === currentHeight) {
                                    noChangeCount++;
                                    console.log(`No new content loaded, attempt ${noChangeCount}/5`);
                                } else {
                                    noChangeCount = 0; // Reset the counter if new content was loaded
                                }
                                
                                // Break early if we've reached the end
                                if (endOfResults) {
                                    console.log('Reached end of results message');
                                    break;
                                }
                            } catch (err) {
                                console.error(`Error during scrolling: ${err.message}`);
                                noChangeCount++;
                            }
                        }
                    }
                    
                    // Extract URLs with better error handling
                    console.log('Extracting business URLs...');
                    let urls = [];
                    try {
                        urls = await page.$$eval('a', links => 
                            links.map(link => link.href)
                                .filter(href => href && href.startsWith('https://www.google.com/maps/place/'))
                                .filter((href, index, self) => self.indexOf(href) === index) // Remove duplicates
                        );
                        
                        console.log(`Found ${urls.length} raw business listings in this query`);
                        
                        // Filter out duplicates already processed in previous queries
                        const newUrls = urls.filter(url => !uniqueUrlsSet.has(url));
                        console.log(`${newUrls.length} are new unique listings (not seen in previous queries)`);
                        
                        // Add new URLs to our tracking set
                        newUrls.forEach(url => uniqueUrlsSet.add(url));
                        
                        urls = newUrls; // Process only new URLs
                        
                        if (urls.length === 0) {
                            console.log('No new business URLs found in this query, moving to the next one');
                            await page.close().catch(err => console.error(`Error closing page: ${err.message}`));
                            continue;
                        }
                    } catch (err) {
                        console.error(`Error extracting URLs: ${err.message}`);
                        
                        // Take a screenshot for debugging
                        await page.screenshot({ path: path.join(dataDir, `urls_debug_${location.name}_q${queryIndex}.png`) });
                        console.log(`Saved debug screenshot to urls_debug_${location.name}_q${queryIndex}.png`);
                        
                        console.log('Skipping this query and continuing with the next one.');
                        await page.close().catch(err => console.error(`Error closing page: ${err.message}`));
                        continue;
                    }
                    
                    // Close the main query page since we now have all URLs
                    await page.close().catch(err => console.error(`Error closing page: ${err.message}`));
                    
                    // Scrape function with better selectors and retry mechanism
                    const scrapePageData = async (url, index, total) => {
                        const progressPct = ((index / total) * 100).toFixed(1);
                        console.log(`Processing business ${index}/${total} (${progressPct}%) for query ${queryIndex + 1}`);
                        
                        // Implement retry mechanism
                        const maxRetries = 2;
                        let retryCount = 0;
                        let success = false;
                        let data = {
                            name: '""', category: '""',
                            address: '""', website: '""', phone: '""', url: `"${url}"`
                        };
                        
                        while (!success && retryCount <= maxRetries) {
                            const newPage = await context.newPage();
                            try {
                                if (retryCount > 0) {
                                    console.log(`Retry attempt ${retryCount} for ${url}`);
                                }
                                
                                await newPage.goto(url, { timeout: 30000, waitUntil: 'domcontentloaded' });
                                
                                // Try different possible selectors
                                try {
                                    await Promise.race([
                                        newPage.waitForSelector('[jstcache="3"]', { timeout: 20000 }),
                                        newPage.waitForSelector('h1', { timeout: 20000 }),
                                        newPage.waitForSelector('div[role="main"]', { timeout: 20000 })
                                    ]);
                                } catch (err) {
                                    console.error(`Error waiting for page content: ${err.message}`);
                                    throw new Error('Page content not loaded properly');
                                }
                                
                                // Wait a moment for dynamic content
                                await newPage.waitForTimeout(1500);
                                
                                // Get business details with error handling and multiple selector attempts
                                const getData = async (selectors, attribute = null) => {
                                    if (!Array.isArray(selectors)) {
                                        selectors = [selectors];
                                    }
                                    
                                    for (const selector of selectors) {
                                        try {
                                            const element = await newPage.$(selector);
                                            if (!element) continue;
                                            
                                            if (attribute) {
                                                const value = await element.getAttribute(attribute);
                                                if (value) return value;
                                            } else {
                                                const value = await element.textContent();
                                                if (value) return value;
                                            }
                                        } catch (err) {
                                            continue;
                                        }
                                    }
                                    return '';
                                };
                                
                                // Expanded selectors for better coverage
                                const name = await getData([
                                    'h1', 
                                    'div[role="main"] h1', 
                                    'div[jstcache] h1',
                                    'div.section-hero-header-title-title'
                                ]) || '';
                                
                                const category = await getData([
                                    'button[jsaction*="pane.rating.category"]',
                                    'button[jsaction*="category"]',
                                    'span[jsan*="category"]',
                                    'button[aria-label*="business"]',
                                    'div.section-result-description',
                                    'div[jstcache] div.fontBodyMedium span'
                                ]) || '';
                                
                                const address = await getData([
                                    'button[data-tooltip="Copy address"]',
                                    'button[aria-label*="address"]',
                                    'button[data-item-id*="address"]',
                                    'div.section-info-line[data-tooltip*="address"]',
                                    'button[jstcache]:has-text("address")'
                                ]) || '';
                                
                                const website = await getData([
                                    'a[data-tooltip="Open website"], a[data-tooltip="Open menu link"]',
                                    'a[aria-label*="website"]',
                                    'a[jsaction*="website"]',
                                    'div.section-info-line a[data-metrics-click*="website"]',
                                    'a[jstcache]:has-text("website")'
                                ], 'href') || '';
                                
                                const phone = await getData([
                                    'button[data-tooltip="Copy phone number"]',
                                    'button[aria-label*="phone"]',
                                    'button[data-item-id*="phone"]',
                                    'div.section-info-line[data-tooltip*="phone"]',
                                    'button[jstcache]:has-text("phone")'
                                ]) || '';
                                
                                // Properly escape CSV fields
                                const escapeCSV = (field) => {
                                    if (!field) return '""';
                                    // Remove any line breaks that would break CSV format
                                    const cleaned = field.replace(/[\r\n]+/g, ' ').trim();
                                    const escaped = cleaned.replace(/"/g, '""');
                                    return `"${escaped}"`;
                                };
                                
                                data = {
                                    name: escapeCSV(name),
                                    category: escapeCSV(category),
                                    address: escapeCSV(address),
                                    website: escapeCSV(website),
                                    phone: escapeCSV(phone),
                                    url: escapeCSV(url)
                                };
                                
                                // Consider success if we have at least a name
                                if (name && name.trim().length > 0) {
                                    success = true;
                                } else {
                                    throw new Error('Failed to extract business name');
                                }
                                
                            } catch (err) {
                                console.error(`Error scraping ${url} (attempt ${retryCount + 1}): ${err.message}`);
                                retryCount++;
                                
                                // Take screenshot on final retry failure
                                if (retryCount === maxRetries) {
                                    try {
                                        await newPage.screenshot({ 
                                            path: path.join(dataDir, `error_business_${locationIndex}_${queryIndex}_${index}.png`) 
                                        });
                                    } catch (screenshotErr) {
                                        console.error(`Error taking screenshot: ${screenshotErr.message}`);
                                    }
                                }
                            } finally {
                                await newPage.close().catch(err => console.error(`Error closing page: ${err.message}`));
                                
                                // Add random delay between requests to avoid rate limiting
                                if (!success && retryCount <= maxRetries) {
                                    const retryDelay = 2000 + Math.floor(Math.random() * 2000);
                                    console.log(`Waiting ${retryDelay}ms before retry...`);
                                    await new Promise(resolve => setTimeout(resolve, retryDelay));
                                }
                            }
                        }
                        
                        return data;
                    };
                    
                    // Batch processing with progress reporting and better error handling
                    let processedCount = 0;
                    
                    // Get or create write streams for appending data
                    const locationCsvStream = fs.createWriteStream(locationOutputFilename, { flags: 'a' });
                    const combinedCsvStream = fs.createWriteStream(combinedOutputFilename, { flags: 'a' });
                    
                    locationCsvStream.on('error', (err) => {
                        console.error(`Error writing to location CSV file: ${err.message}`);
                    });
                    
                    combinedCsvStream.on('error', (err) => {
                        console.error(`Error writing to combined CSV file: ${err.message}`);
                    });
                    
                    // Process URLs in batches
                    for (let i = 0; i < urls.length; i += batchSize) {
                        const batchUrls = urls.slice(i, i + batchSize);
                        console.log(`Starting batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(urls.length / batchSize)} for query ${queryIndex + 1}`);
                        
                        const batchPromises = batchUrls.map((url, idx) => 
                            scrapePageData(url, i + idx + 1, urls.length)
                        );
                        
                        // Use Promise.allSettled to handle errors gracefully
                        const batchResults = await Promise.allSettled(batchPromises);
                        
                        // Process results (including failed ones)
                        for (const result of batchResults) {
                            if (result.status === 'fulfilled') {
                                const data = result.value;
                                
                                // Only save if we have at least a name or address
                                const hasData = (
                                    (data.name && data.name !== '""') || 
                                    (data.address && data.address !== '""')
                                );
                                
                                if (hasData) {
                                    // Write to location-specific file
                                    const locationCsvRow = `${data.name},${data.category},${data.address},${data.website},${data.phone},${data.url}\n`;
                                    locationCsvStream.write(locationCsvRow);
                                    
                                    // Write to combined file with location name
                                    const combinedCsvRow = `"${location.name}",${data.name},${data.category},${data.address},${data.website},${data.phone},${data.url}\n`;
                                    combinedCsvStream.write(combinedCsvRow);
                                }
                                
                                processedCount++;
                            }
                        }
                        
                        console.log(`Completed ${processedCount}/${urls.length} businesses (${((processedCount / urls.length) * 100).toFixed(1)}%) for query ${queryIndex + 1}`);
                        
                        // Add a delay between batches to avoid rate limiting
                        // Vary delay based on batch size
                        if (i + batchSize < urls.length) {
                            const batchDelay = 3000 + Math.floor(Math.random() * 2000);
                            console.log(`Pausing for ${batchDelay}ms before next batch...`);
                            await new Promise(resolve => setTimeout(resolve, batchDelay));
                        }
                    }
                    
                    // End this query's processing
                    console.log(`Completed processing query ${queryIndex + 1} for ${location.name}, found ${processedCount} businesses`);
                    
                    // Add larger delay between queries
                    if (queryIndex + 1 < location.queries.length) {
                        const queryDelay = 5000 + Math.floor(Math.random() * 5000);
                        console.log(`Waiting ${queryDelay}ms before next query...`);
                        await new Promise(resolve => setTimeout(resolve, queryDelay));
                    }
                    
                } catch (err) {
                    console.error(`Script error for query ${queryIndex + 1} in ${location.name}: ${err.message}`);
                }
            } // End of query loop
            
            console.log(`\nCompleted all queries for ${location.name}, found ${uniqueUrlsSet.size} unique businesses`);
            console.timeEnd(`Location Execution Time: ${location.name}`);
            
            // Add a larger delay between locations to avoid detection
            if (locationIndex + 1 < locations.length) {
                const locationDelay = 10000 + Math.floor(Math.random() * 5000);
                console.log(`\nWaiting ${locationDelay}ms before processing next location...`);
                await new Promise(resolve => setTimeout(resolve, locationDelay));
            }
            
        } // End of location loop
        
        await browser.close().catch(err => console.error(`Error closing browser: ${err.message}`));
        
    } catch (err) {
        console.error(`Fatal script error: ${err.message}`);
    } finally {
        console.timeEnd("Total Execution Time");
        console.log(`Multi-location scraping completed at ${new Date().toLocaleString()}`);
    }
})().catch(err => {
    console.error(`Fatal error: ${err.message}`);
    process.exit(1);
});