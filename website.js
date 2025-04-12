const playwright = require('playwright');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

// ========== CONFIGURATION ========== //
const config = {
  dataDir: path.join(__dirname, 'data'),
  inputFilename: 'home_dec_toledo_spain_2025-04-09_19-46.csv',
  outputFilename: 'home_dec_toledo_spain_2025-04-09_19-46_emails.csv',
  requestDelay: 300,
  navigationTimeout: 8000,
  maxRetries: 3,
  parallelScrapers: Math.max(1, Math.min(os.cpus().length - 1, 8)),
  headless: true,
  debug: true,
  limit: 0,
  maxEmailsPerSite: 3,
  removeRowsWithoutEmails: true,
  useWorkerThreads: true,
  contactPageKeywords: ['contact', 'about', 'reach', 'connect', 'email', 'get in touch'],
  cacheResults: true,
  browserOptions: {
    ignoreHTTPSErrors: true,
    args: [
      '--disable-http2',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-extensions',
      '--disable-web-security',
      '--disable-features=site-per-process',
      '--disable-accelerated-video-decode',
      '--disable-accelerated-mjpeg-decode',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-breakpad',
      '--disable-component-extensions-with-background-pages',
      '--disable-features=TranslateUI',
      '--disable-ipc-flooding-protection',
      '--disable-renderer-backgrounding',
      '--enable-features=NetworkService,NetworkServiceInProcess'
    ],
  }
};

// Enhanced email filters
const emailFilters = [
  '@example.com', 'noreply@', 'no-reply@', 'donotreply@',
  'sentry.io', 'wixpress.com', '.png', '.jpg', '.jpeg', '.gif', '.svg',
  'info@', 'sales@', 'support@', 'hello@', 'contact@',
  'admin@', 'webmaster@', 'customerservice@', 'service@', 'help@',
  'notifications@', 'alerts@', 'newsletter@', 'signup@', 'mail@'
];

// Pattern to detect URL encoded emails and SVG filenames
const invalidEmailPatterns = [
  /%[0-9A-F]{2}/, // URL encoded characters
  /\.svg$/i,      // SVG files
  /icon/i,        // Icon references
  /image/i,       // Image references
  /^[0-9]+$/,     // Just numbers
  /^undefined$/   // Undefined values
];

// Email cache to avoid re-scraping the same domains
const emailCache = new Map();

// ========== WORKER THREAD IMPLEMENTATION ========== //
if (!isMainThread && workerData) {
  // This code runs in worker threads
  const { urls, workerId } = workerData;
  const results = {};
  
  async function workerScrapeEmails(url) {
    let browser = null;
    
    try {
      browser = await playwright.chromium.launch({ 
        headless: true,
        args: config.browserOptions.args
      });
      
      const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        viewport: { width: 1280, height: 800 },
        ignoreHTTPSErrors: true,
        javaScriptEnabled: true,
        bypassCSP: true
      });
      
      const page = await context.newPage();
      page.setDefaultNavigationTimeout(config.navigationTimeout);
      
      // Block resource types for faster loading
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot,css,mp4,webm,ogg,mp3,wav,pdf}', route => route.abort());
      
      // Extract domain for caching purposes
      const domain = extractDomain(url);
      const emails = await scrapeWebsiteForEmails(page, url);
      
      await context.close();
      await browser.close();
      
      return emails.length > 0 ? emails.join(', ') : 'NA';
    } catch (error) {
      if (browser) await browser.close();
      return 'NA';
    }
  }
  
  async function scrapeWebsiteForEmails(page, url) {
    try {
      // Set a tight timeout for better performance
      await page.goto(url, { 
        waitUntil: 'domcontentloaded', 
        timeout: config.navigationTimeout 
      });
      
      // Collect emails from homepage
      let emails = await extractEmailsFromPage(page);
      
      // Try contact page if not enough emails found
      if (emails.length < 2) {
        const contactUrl = await findContactPageUrl(page, url);
        if (contactUrl && contactUrl !== url) {
          try {
            await page.goto(contactUrl, { 
              waitUntil: 'domcontentloaded',
              timeout: config.navigationTimeout 
            });
            
            const contactEmails = await extractEmailsFromPage(page);
            emails.push(...contactEmails);
          } catch (e) {
            // Continue if contact page fails
          }
        }
      }
      
      // Rank and limit results
      emails = [...new Set(emails)]; // Remove duplicates
      const rankedEmails = rankEmails(emails, url);
      return rankedEmails.slice(0, config.maxEmailsPerSite || 3);
    } catch (e) {
      return [];
    }
  }
  
  async function extractEmailsFromPage(page) {
    try {
      // Extract from mailto links (fastest method)
      const mailtoEmails = await page.$$eval('a[href^="mailto:"]', links => 
        links.map(link => link.href.replace('mailto:', '').split('?')[0].toLowerCase())
      );
      
      // Extract from page text
      const textContent = await page.evaluate(() => document.body.innerText || '');
      const textEmails = extractEmails(textContent);
      
      // Only check HTML if necessary
      let contentEmails = [];
      if (mailtoEmails.length < 2 && textEmails.length < 2) {
        const html = await page.content();
        contentEmails = extractEmails(html);
      }
      
      return [...mailtoEmails, ...textEmails, ...contentEmails];
    } catch (e) {
      return [];
    }
  }
  
  async function findContactPageUrl(page, baseUrl) {
    try {
      // Find all anchors that might be contact links
      const contactLinks = await page.$$eval('a', (links, keywords) => {
        return links
          .filter(link => {
            const text = (link.innerText || '').toLowerCase();
            const href = (link.href || '').toLowerCase();
            return keywords.some(keyword => text.includes(keyword) || href.includes(keyword));
          })
          .map(link => link.href)
          .slice(0, 3); // Only get the first few matches
      }, config.contactPageKeywords);
      
      if (contactLinks.length) return contactLinks[0];
      
      // Try common contact paths
      try {
        for (const path of ['/contact', '/contact-us', '/about', '/about-us']) {
          const url = new URL(path, baseUrl).href;
          const response = await page.request.head(url, { timeout: 2000 }).catch(() => null);
          if (response?.status() === 200) return url;
        }
      } catch (e) {}
      
      return null;
    } catch (e) {
      return null;
    }
  }
  
  // Process all URLs assigned to this worker
  (async () => {
    for (const url of urls) {
      results[url] = await workerScrapeEmails(url);
      // Send progress updates
      parentPort.postMessage({ type: 'progress', url, result: results[url] });
    }
    
    // Send final results
    parentPort.postMessage({ type: 'complete', results });
  })();
  
  // Helper functions
  function extractDomain(url) {
    try {
      const urlObj = new URL(url.startsWith('http') ? url : `https://${url}`);
      return urlObj.hostname;
    } catch (e) {
      return url;
    }
  }
  
  function extractEmails(text) {
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    let emails = text.match(emailRegex) || [];
    
    // Convert to lowercase and filter out invalid emails
    emails = emails
      .map(e => e.toLowerCase())
      .filter(email => {
        // Filter out email addresses containing any string from emailFilters
        if (emailFilters.some(filter => email.includes(filter.toLowerCase()))) {
          return false;
        }
        
        // Filter out emails matching invalid patterns
        if (invalidEmailPatterns.some(pattern => pattern.test(email))) {
          return false;
        }
        
        // Make sure it's a proper email with alphanumeric characters and common symbols
        if (!/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(email)) {
          return false;
        }
        
        return true;
      });
    
    return [...new Set(emails)]; // Remove duplicates
  }
  
  function rankEmails(emails, domainName) {
    let siteDomain = '';
    try {
      const urlObj = new URL(domainName.startsWith('http') ? domainName : `https://${domainName}`);
      siteDomain = urlObj.hostname.replace('www.', '');
    } catch (e) {
      siteDomain = domainName.replace(/^https?:\/\//, '').replace('www.', '').split('/')[0];
    }
    
    return emails
      .map(email => {
        const emailParts = email.split('@');
        const emailDomain = emailParts[1];
        const emailUser = emailParts[0];
        
        let score = 0;
        if (emailDomain?.includes(siteDomain)) score += 100;
        if (/^[a-z](\.[a-z]+)?$/.test(emailUser)) score += 50;
        if (emailUser.includes('.')) score += 30;
        if (['info', 'contact', 'hello', 'support', 'sales', 'admin'].includes(emailUser)) score -= 20;
        
        return { email, score };
      })
      .sort((a, b) => b.score - a.score)
      .map(item => item.email);
  }
} else {
  // ========== MAIN THREAD CODE ========== //
  
  // Helper functions for email extraction
  function normalizeUrl(url) {
    if (!url?.trim()) return null;
    url = url.trim();
    
    // Clean URL
    if (!url.startsWith('http')) url = 'https://' + url;
    
    try {
      const urlObj = new URL(url);
      // Just return the domain to improve caching hits
      return urlObj.origin;
    } catch (e) {
      return url;
    }
  }
  
  function extractDomain(url) {
    try {
      const urlObj = new URL(url.startsWith('http') ? url : `https://${url}`);
      return urlObj.hostname;
    } catch (e) {
      return url.replace(/^https?:\/\//, '').split('/')[0];
    }
  }
  
  function extractEmails(text) {
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    let emails = text.match(emailRegex) || [];
    
    // Convert to lowercase and filter out invalid emails
    emails = emails
      .map(e => e.toLowerCase())
      .filter(email => {
        // Filter out email addresses containing any string from emailFilters
        if (emailFilters.some(filter => email.includes(filter.toLowerCase()))) {
          return false;
        }
        
        // Filter out emails matching invalid patterns
        if (invalidEmailPatterns.some(pattern => pattern.test(email))) {
          return false;
        }
        
        // Make sure it's a proper email with alphanumeric characters and common symbols
        if (!/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(email)) {
          return false;
        }
        
        return true;
      });
    
    return [...new Set(emails)]; // Remove duplicates
  }
  
  function rankEmails(emails, domainName) {
    let siteDomain = extractDomain(domainName);
    
    return emails
      .map(email => {
        const emailParts = email.split('@');
        const emailDomain = emailParts[1];
        const emailUser = emailParts[0];
        
        let score = 0;
        if (emailDomain?.includes(siteDomain)) score += 100;
        if (/^[a-z](\.[a-z]+)?$/.test(emailUser)) score += 50;
        if (emailUser.includes('.')) score += 30;
        if (['info', 'contact', 'hello', 'support', 'sales', 'admin'].includes(emailUser)) score -= 20;
        
        return { email, score };
      })
      .sort((a, b) => b.score - a.score)
      .map(item => item.email);
  }
  
  // Distributed scraping with worker threads
  async function scrapeWithWorkers(urls) {
    // Filter out cached domains to avoid redundant scraping
    const domainsToScrape = new Map();
    const results = {};
    
    // Check cache first
    for (const url of urls) {
      const normalizedUrl = normalizeUrl(url);
      if (!normalizedUrl) {
        results[url] = 'NA';
        continue;
      }
      
      const domain = extractDomain(normalizedUrl);
      
      if (emailCache.has(domain)) {
        results[url] = emailCache.get(domain);
      } else {
        domainsToScrape.set(url, normalizedUrl);
      }
    }
    
    // If everything was cached, return early
    if (domainsToScrape.size === 0) {
      return results;
    }
    
    const urlsToScrape = Array.from(domainsToScrape.keys());
    const workerCount = Math.min(config.parallelScrapers, urlsToScrape.length);
    const workerPromises = [];
    
    // Split tasks evenly among workers
    for (let i = 0; i < workerCount; i++) {
      const workerUrls = urlsToScrape.filter((_, index) => index % workerCount === i);
      
      const worker = new Worker(__filename, {
        workerData: {
          urls: workerUrls,
          workerId: i
        }
      });
      
      const workerPromise = new Promise((resolve) => {
        worker.on('message', (message) => {
          if (message.type === 'progress') {
            results[message.url] = message.result;
            
            // Update cache
            if (message.result !== 'NA' && config.cacheResults) {
              const domain = extractDomain(message.url);
              emailCache.set(domain, message.result);
            }
            
            if (config.debug) {
              console.log(`Worker ${i}: Processed ${message.url} (${message.result})`);
            }
          } else if (message.type === 'complete') {
            resolve();
          }
        });
        
        worker.on('error', () => resolve());
        worker.on('exit', () => resolve());
      });
      
      workerPromises.push(workerPromise);
    }
    
    // Wait for all workers to complete
    await Promise.all(workerPromises);
    return results;
  }
  
  // ========== CSV PROCESSING ========== //
  async function processCsv() {
    const rows = [];
    let originalHeaders = [];
    
    return new Promise((resolve, reject) => {
      fs.createReadStream(path.join(config.dataDir, config.inputFilename))
        .pipe(csv())
        .on('headers', (headers) => originalHeaders = headers)
        .on('data', (row) => rows.push(row))
        .on('end', async () => {
          // Apply limit if set
          let processRows = rows;
          if (config.limit > 0 && config.limit < rows.length) {
            processRows = rows.slice(0, config.limit);
            console.log(`Processing ${processRows.length} websites (limited from ${rows.length})...`);
          } else {
            console.log(`Processing ${rows.length} websites...`);
          }
          
          const websiteCol = originalHeaders.find(h => 
            h.toLowerCase().includes('website') || h.toLowerCase().includes('url')
          ) || 'Website';
          
          // Extract all URLs for parallel processing
          const websiteUrls = processRows.map(row => row[websiteCol] || '').filter(Boolean);
          console.log(`Found ${websiteUrls.length} websites to scrape...`);
          
          // Parallel scrape all websites
          let emailResults;
          if (config.useWorkerThreads) {
            console.log(`Using ${config.parallelScrapers} worker threads for parallel scraping...`);
            emailResults = await scrapeWithWorkers(websiteUrls);
          } else {
            // Fallback to sequential processing
            emailResults = {};
            for (const url of websiteUrls) {
              emailResults[url] = await scrapeSequential(url);
            }
          }
          
          // Add results back to the rows
          for (const row of processRows) {
            const website = row[websiteCol] || '';
            row['Emails'] = website ? (emailResults[website] || 'NA') : 'NA';
          }
          
          // Filter out rows without emails if configured
          let finalRows = processRows;
          const remainingRows = config.limit > 0 ? rows.slice(config.limit) : [];
          
          if (config.removeRowsWithoutEmails) {
            finalRows = processRows.filter(row => row['Emails'] && row['Emails'] !== 'NA');
            console.log(`Removed ${processRows.length - finalRows.length} rows without valid emails`);
          }
          
          // Add Emails column after Website column
          const websiteIndex = originalHeaders.indexOf(websiteCol);
          const headers = [...originalHeaders];
          if (websiteIndex >= 0) headers.splice(websiteIndex + 1, 0, 'Emails');
          else headers.push('Emails');
          
          resolve({ headers, rows: [...finalRows, ...remainingRows] });
        })
        .on('error', reject);
    });
  }
  
  // Simple sequential scraping for fallback
  async function scrapeSequential(url) {
    // Implementation omitted - only used as fallback if worker threads fail
    return 'NA';
  }
  
  async function writeResults(data) {
    const outputPath = path.join(config.dataDir, config.outputFilename);
    const csvWriter = createCsvWriter({
      path: outputPath,
      header: data.headers.map(header => ({ id: header, title: header }))
    });
    
    await csvWriter.writeRecords(data.rows);
    console.log(`✅ Results saved to: ${outputPath}`);
  }
  
  // ========== COMMAND LINE ARGS PROCESSING ========== //
  function processArgs() {
    const args = process.argv.slice(2);
    
    for (let i = 0; i < args.length; i++) {
      if (args[i] === '--limit' && i + 1 < args.length) {
        const limitValue = parseInt(args[i + 1]);
        if (!isNaN(limitValue) && limitValue > 0) {
          config.limit = limitValue;
          console.log(`Limit set to process ${limitValue} websites`);
        }
      } else if (args[i] === '--max-emails' && i + 1 < args.length) {
        const maxEmails = parseInt(args[i + 1]);
        if (!isNaN(maxEmails) && maxEmails >= 0) {
          config.maxEmailsPerSite = maxEmails;
          console.log(`Maximum emails per site set to ${maxEmails}`);
        }
      } else if (args[i] === '--input' && i + 1 < args.length) {
        config.inputFilename = args[i + 1];
      } else if (args[i] === '--output' && i + 1 < args.length) {
        config.outputFilename = args[i + 1];
      } else if (args[i] === '--parallel' && i + 1 < args.length) {
        const parallelValue = parseInt(args[i + 1]);
        if (!isNaN(parallelValue) && parallelValue > 0) {
          config.parallelScrapers = parallelValue;
          console.log(`Parallel scrapers set to ${parallelValue}`);
        }
      } else if (args[i] === '--no-limit') {
        config.maxEmailsPerSite = 0;
        console.log('Email results will not be limited');
      } else if (args[i] === '--keep-empty') {
        config.removeRowsWithoutEmails = false;
        console.log('Keeping rows without emails in the output');
      } else if (args[i] === '--no-workers') {
        config.useWorkerThreads = false;
        console.log('Disabled worker threads, using sequential processing');
      } else if (args[i] === '--no-cache') {
        config.cacheResults = false;
        console.log('Disabled result caching');
      }
    }
  }
  
  // ========== MAIN EXECUTION ========== //
  async function main() {
    try {
      console.log('🚀 Starting email scraper with optimized settings...');
      console.time('Total execution time');
      
      // Process command-line arguments
      processArgs();
      
      // Check data directory exists
      if (!fs.existsSync(config.dataDir)) {
        fs.mkdirSync(config.dataDir, { recursive: true });
      }
      
      const inputPath = path.join(config.dataDir, config.inputFilename);
      if (!fs.existsSync(inputPath)) throw new Error(`Input file not found: ${inputPath}`);
      
      const result = await processCsv();
      await writeResults(result);
      
      console.timeEnd('Total execution time');
      console.log('🎉 Scraping completed successfully!');
    } catch (error) {
      console.error('❌ Error:', error.message);
    }
  }
  
  // Only run in main thread
  if (isMainThread) {
    main();
  }
}