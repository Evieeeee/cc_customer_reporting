// ContentClicks Dashboard JavaScript

// ============================================================================
// GLOBAL STATE
// ============================================================================

let currentCustomer = null;
let currentMetrics = null;
let charts = {
    socialMedia: null,
    website: null,
    email: null
};

// Customer journey stages configuration
const JOURNEY_STAGES = {
    awareness: { color: '#006039', label: 'Awareness' },
    engagement: { color: '#A37E2C', label: 'Engagement' },
    conversion: { color: '#00804d', label: 'Conversion' },
    response: { color: '#C4A156', label: 'Response' },
    retention: { color: '#826419', label: 'Retention' },
    advocacy: { color: '#004428', label: 'Advocacy' },
    quality: { color: '#9E9E9E', label: 'Quality' }
};

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    initializeEventListeners();
    initializePageDiscovery();
    loadCustomers();
});

function initializeEventListeners() {
    // Customer selector
    document.getElementById('customerSelect').addEventListener('change', (e) => {
        if (e.target.value) {
            loadCustomerData(e.target.value);  // String ID, not parseInt
        } else {
            showNoCustomerMessage();
        }
    });
    
    // Add customer button
    document.getElementById('addCustomerBtn').addEventListener('click', openAddCustomerModal);
    
    // Modal close buttons
    document.querySelectorAll('.close-modal, .cancel-modal').forEach(btn => {
        btn.addEventListener('click', closeAddCustomerModal);
    });
    
    // Click outside modal to close
    document.getElementById('addCustomerModal').addEventListener('click', (e) => {
        if (e.target.id === 'addCustomerModal') {
            closeAddCustomerModal();
        }
    });
    
    // Add customer form submit
    document.getElementById('addCustomerForm').addEventListener('submit', handleAddCustomer);
    
    // Refresh button
    document.getElementById('refreshBtn').addEventListener('click', () => {
        if (currentCustomer) {
            refreshCustomerData(currentCustomer.id);
        }
    });
    
    // Export PDF button
    document.getElementById('exportPdfBtn').addEventListener('click', exportToPDF);
}

// ============================================================================
// CUSTOMER MANAGEMENT
// ============================================================================

async function loadCustomers() {
    try {
        const response = await fetch('/api/customers');
        const data = await response.json();
        
        if (data.success) {
            const select = document.getElementById('customerSelect');
            select.innerHTML = '<option value="">Select Customer...</option>';
            
            data.customers.forEach(customer => {
                const option = document.createElement('option');
                option.value = customer.id;
                option.textContent = `${customer.name} (${customer.industry})`;
                select.appendChild(option);
            });
        }
    } catch (error) {
        showToast('Error loading customers', 'error');
        console.error('Error:', error);
    }
}

async function loadCustomerData(customerId) {
    showLoading(true);
    
    try {
        // Load customer details and metrics
        const [customerResponse, metricsResponse] = await Promise.all([
            fetch(`/api/customers/${customerId}`),
            fetch(`/api/customers/${customerId}/metrics`)
        ]);
        
        const customerData = await customerResponse.json();
        const metricsData = await metricsResponse.json();
        
        if (customerData.success && metricsData.success) {
            currentCustomer = customerData.customer;
            currentMetrics = metricsData.metrics;
            
            renderDashboard();
        }
    } catch (error) {
        showToast('Error loading customer data', 'error');
        console.error('Error:', error);
    } finally {
        showLoading(false);
    }
}

async function refreshCustomerData(customerId) {
    try {
        // Get historical collection checkbox state
        const collectHistory = document.getElementById('collectHistoryCheckbox').checked;
        
        // Show loading overlay with custom message
        const overlay = document.getElementById('loadingOverlay');
        const loadingText = overlay.querySelector('p');
        
        if (collectHistory) {
            loadingText.textContent = 'Collecting 12 months of historical data... This may take 5-10 minutes.';
        } else {
            loadingText.textContent = 'Collecting data from social media, email, and website...';
        }
        
        overlay.classList.add('active');
        
        showToast('Starting data collection...', 'info');
        
        // Trigger data collection with historical flag
        const response = await fetch(`/api/customers/${customerId}/collect`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                days: 30,
                collect_history: collectHistory 
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Poll for completion
            pollForDataCompletion(customerId);
        } else {
            overlay.classList.remove('active');
            showToast('Error starting data collection', 'error');
        }
    } catch (error) {
        document.getElementById('loadingOverlay').classList.remove('active');
        showToast('Error refreshing data', 'error');
        console.error('Error:', error);
    }
}

// Poll the backend to check data collection status
let pollAttempts = 0;
const MAX_POLL_ATTEMPTS = 60; // 60 attempts * 2 seconds = 2 minutes max
let completedSources = new Set(); // Track which sources have been loaded

async function pollForDataCompletion(customerId) {
    const overlay = document.getElementById('loadingOverlay');
    const loadingText = overlay.querySelector('p');
    
    pollAttempts++;
    
    if (pollAttempts > MAX_POLL_ATTEMPTS) {
        overlay.classList.remove('active');
        showToast('Data collection is taking longer than expected. Please check back in a few minutes.', 'warning');
        pollAttempts = 0;
        completedSources.clear();
        return;
    }
    
    try {
        // Check collection status from the status endpoint
        const statusResponse = await fetch(`/api/customers/${customerId}/collect/status`);
        const statusData = await statusResponse.json();
        
        if (statusData.success && statusData.status) {
            const status = statusData.status;
            
            // Update loading message with actual status
            if (status.message) {
                loadingText.textContent = status.message;
            }
            
            // Show detailed progress if available
            if (status.sources) {
                let details = [];
                let newCompletions = [];
                
                for (const [source, data] of Object.entries(status.sources)) {
                    if (data.status === 'completed') {
                        details.push(`âœ… ${source.replace('_', ' ')}`);
                        
                        // Check if this source just completed
                        if (!completedSources.has(source)) {
                            newCompletions.push(source);
                            completedSources.add(source);
                        }
                    } else if (data.status === 'collecting') {
                        details.push(`â³ ${source.replace('_', ' ')}`);
                    } else if (data.status === 'failed') {
                        details.push(`âŒ ${source.replace('_', ' ')}`);
                        // Mark failed sources as "completed" so we don't wait for them
                        if (!completedSources.has(source)) {
                            completedSources.add(source);
                        }
                    }
                }
                
                if (details.length > 0) {
                    loadingText.innerHTML = status.message + '<br><br>' + details.join(' â€¢ ');
                }
                
                // If any NEW sources completed, refresh the dashboard
                if (newCompletions.length > 0) {
                    console.log(`[INFO] New sources completed: ${newCompletions.join(', ')}`);
                    showToast(`${newCompletions.length} more section(s) ready! Updating...`, 'success');
                    loadCustomerData(customerId);
                    // Keep polling for the rest
                }
            }
            
            // Check if completely done
            if (status.completed) {
                // All data collection is complete!
                overlay.classList.remove('active');
                pollAttempts = 0;
                completedSources.clear();
                
                showToast('âœ… All data collection complete!', 'success');
                
                // Final refresh to show all data
                setTimeout(() => {
                    loadCustomerData(customerId);
                }, 500);
                return;
            }
            
            // Check if error
            if (status.status === 'error') {
                overlay.classList.remove('active');
                pollAttempts = 0;
                completedSources.clear();
                showToast('Data collection error: ' + (status.error || 'Unknown error'), 'error');
                return;
            }
            
            // Still in progress, continue polling every 2 seconds
            setTimeout(() => {
                pollForDataCompletion(customerId);
            }, 2000);
        } else {
            // No status available, poll again
            setTimeout(() => {
                pollForDataCompletion(customerId);
            }, 2000);
        }
    } catch (error) {
        console.error('Error checking collection status:', error);
        // Continue polling even if there's an error
        setTimeout(() => {
            pollForDataCompletion(customerId);
        }, 2000);
    }
}

// Legacy function - no longer needed but keeping for compatibility
function hasNewData(metrics) {
    // Check if any medium has non-zero values
    for (const medium in metrics) {
        const mediumData = metrics[medium];
        for (const stage in mediumData) {
            const kpis = mediumData[stage];
            for (const kpi of kpis) {
                if (kpi.kpi_value && kpi.kpi_value > 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

function openAddCustomerModal() {
    document.getElementById('addCustomerModal').classList.add('active');
}

function closeAddCustomerModal() {
    document.getElementById('addCustomerModal').classList.remove('active');
    document.getElementById('addCustomerForm').reset();
    
    // Reset page selection
    document.getElementById('pageSelectionArea').style.display = 'none';
    document.getElementById('availablePages').innerHTML = '';
    discoveredPages = [];
    selectedPageIds = [];
}

async function handleAddCustomer(e) {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const customerData = {
        name: formData.get('name'),
        industry: formData.get('industry'),
        credentials: {
            social_media: {},
            email: {},
            website: {}
        }
    };
    
    // Add credentials if provided
    const systemToken = document.getElementById('systemUserToken').value;
    if (systemToken) {
        customerData.credentials.social_media.system_user_token = systemToken;
        
        // Add selected page IDs if pages were discovered
        if (selectedPageIds.length > 0) {
            customerData.credentials.social_media.selected_page_ids = selectedPageIds.join(',');
        }
    }
    
    const instantlyKey = document.getElementById('instantlyApiKey').value;
    if (instantlyKey) {
        customerData.credentials.email.instantly_api_key = instantlyKey;
    }
    
    const klaviyoKey = document.getElementById('klaviyoApiKey').value;
    if (klaviyoKey) {
        customerData.credentials.email.klaviyo_api_key = klaviyoKey;
    }
    
    const ga4PropertyId = document.getElementById('ga4PropertyId').value;
    if (ga4PropertyId) {
        customerData.credentials.website.ga4_property_id = ga4PropertyId;
    }
    
    try {
        showLoading(true);
        
        const response = await fetch('/api/customers', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(customerData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('Customer created successfully!', 'success');
            closeAddCustomerModal();
            await loadCustomers();
            
            // Select the new customer
            document.getElementById('customerSelect').value = data.customer.id;
            loadCustomerData(data.customer.id);
        } else {
            showToast('Error creating customer: ' + data.error, 'error');
        }
    } catch (error) {
        showToast('Error creating customer', 'error');
        console.error('Error:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// DASHBOARD RENDERING
// ============================================================================

function showNoCustomerMessage() {
    document.getElementById('noCustomerMessage').style.display = 'flex';
    document.getElementById('dashboardContent').style.display = 'none';
    currentCustomer = null;
    currentMetrics = null;
}

function renderDashboard() {
    document.getElementById('noCustomerMessage').style.display = 'none';
    document.getElementById('dashboardContent').style.display = 'block';
    
    if (!currentMetrics) {
        showToast('No metrics available. Click Refresh to collect data.', 'info');
        return;
    }
    
    // Render each section
    renderSocialMediaSection();
    renderWebsiteSection();
    renderEmailSection();
}

function renderSocialMediaSection() {
    const metrics = currentMetrics.social_media || {};
    const container = document.querySelector('#socialMediaSection .kpi-cards-container');
    
    container.innerHTML = '';
    
    // Create KPI cards for each journey stage
    const stages = ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'];
    
    stages.forEach(stage => {
        if (metrics[stage]) {
            const stageData = metrics[stage];
            const kpiName = Object.keys(stageData)[0];
            const kpiData = stageData;
            
            const card = createKPICard(
                'social_media',
                stage,
                kpiData.kpi_name || kpiName,
                kpiData.kpi_value || 0,
                kpiData.benchmark_value || 0,
                kpiData.time_period_days || 30
            );
            
            container.appendChild(card);
        }
    });
    
    // Load top performers
    loadTopPerformers('social_media', 'topSocialPosts');
}

function renderWebsiteSection() {
    const metrics = currentMetrics.website || {};
    const container = document.querySelector('#websiteSection .kpi-cards-container');
    
    container.innerHTML = '';
    
    const stages = ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'];
    
    stages.forEach(stage => {
        if (metrics[stage]) {
            const stageData = metrics[stage];
            const kpiName = Object.keys(stageData)[0];
            const kpiData = stageData;
            
            const card = createKPICard(
                'website',
                stage,
                kpiData.kpi_name || kpiName,
                kpiData.kpi_value || 0,
                kpiData.benchmark_value || 0,
                kpiData.time_period_days || 30
            );
            
            container.appendChild(card);
        }
    });
    
    loadTopPerformers('website', 'topPages');
}

function renderEmailSection() {
    const metrics = currentMetrics.email || {};
    const container = document.querySelector('#emailSection .kpi-cards-container');
    
    container.innerHTML = '';
    
    const stages = ['awareness', 'engagement', 'response', 'retention', 'quality'];
    
    stages.forEach(stage => {
        if (metrics[stage]) {
            const stageData = metrics[stage];
            const kpiName = Object.keys(stageData)[0];
            const kpiData = stageData;
            
            const card = createKPICard(
                'email',
                stage,
                kpiData.kpi_name || kpiName,
                kpiData.kpi_value || 0,
                kpiData.benchmark_value || 0,
                kpiData.time_period_days || 30
            );
            
            container.appendChild(card);
        }
    });
}

function createKPICard(medium, journeyStage, kpiName, kpiValue, benchmarkValue, timePeriodDays) {
    const card = document.createElement('div');
    card.className = 'kpi-card';
    card.dataset.medium = medium;
    card.dataset.journeyStage = journeyStage;
    card.dataset.kpiName = kpiName;
    
    // Calculate performance vs benchmark
    let performance = 'met';
    let icon = '=';
    let text = 'Benchmark Met';
    
    if (kpiValue > benchmarkValue * 1.1) {
        performance = 'above';
        icon = 'â–²';
        text = `${Math.round((kpiValue / benchmarkValue - 1) * 100)}% Above Benchmark`;
    } else if (kpiValue < benchmarkValue * 0.9) {
        performance = 'below';
        icon = 'â–¼';
        text = `${Math.round((1 - kpiValue / benchmarkValue) * 100)}% Below Benchmark`;
    }
    
    const stageConfig = JOURNEY_STAGES[journeyStage] || { color: '#999', label: journeyStage };
    
    card.innerHTML = `
        <div class="kpi-card-header">
            <div class="kpi-name">${kpiName}</div>
            <div class="journey-stage" style="background-color: ${stageConfig.color}">
                ${stageConfig.label}
            </div>
        </div>
        <div class="kpi-value">${formatKPIValue(kpiValue, kpiName)}</div>
        <div class="time-period">Last ${timePeriodDays} days</div>
        <div class="benchmark-indicator ${performance}">
            <span class="benchmark-icon">${icon}</span>
            <span class="benchmark-text">${text}</span>
        </div>
    `;
    
    // Add click handler to show historical chart
    card.addEventListener('click', () => {
        document.querySelectorAll('.kpi-card').forEach(c => c.classList.remove('active'));
        card.classList.add('active');
        loadHistoricalChart(medium, journeyStage, kpiName);
    });
    
    return card;
}

function formatKPIValue(value, kpiName) {
    // Check if it's a percentage metric
    if (kpiName.toLowerCase().includes('rate') || 
        kpiName.toLowerCase().includes('percentage') ||
        kpiName.toLowerCase().includes('score')) {
        return value.toFixed(1) + '%';
    }
    
    // Format large numbers
    if (value >= 1000000) {
        return (value / 1000000).toFixed(1) + 'M';
    } else if (value >= 1000) {
        return (value / 1000).toFixed(1) + 'K';
    }
    
    return Math.round(value).toLocaleString();
}

// ============================================================================
// CHARTS
// ============================================================================

async function loadHistoricalChart(medium, journeyStage, kpiName) {
    try {
        const response = await fetch(
            `/api/customers/${currentCustomer.id}/metrics/history?` +
            `medium=${medium}&journey_stage=${journeyStage}&kpi_name=${encodeURIComponent(kpiName)}&limit=30`
        );
        
        const data = await response.json();
        
        if (data.success && data.history.length > 0) {
            renderChart(medium, data.history, kpiName);
        }
    } catch (error) {
        console.error('Error loading chart data:', error);
    }
}

function renderChart(medium, history, kpiName) {
    const canvasId = medium === 'social_media' ? 'socialMediaChart' : 
                     medium === 'website' ? 'websiteChart' : 'emailChart';
    
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    // Destroy existing chart
    if (charts[medium === 'social_media' ? 'socialMedia' : medium]) {
        charts[medium === 'social_media' ? 'socialMedia' : medium].destroy();
    }
    
    // Prepare data
    const labels = history.map(h => new Date(h.recorded_at).toLocaleDateString());
    const actualValues = history.map(h => h.kpi_value);
    const benchmarkValues = history.map(h => h.benchmark_value);
    
    // Create chart
    const chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: kpiName,
                    data: actualValues,
                    borderColor: '#006039',
                    backgroundColor: 'rgba(0, 96, 57, 0.1)',
                    tension: 0.4,
                    fill: true,
                    pointRadius: 4,
                    pointHoverRadius: 6
                },
                {
                    label: 'Benchmark',
                    data: benchmarkValues,
                    borderColor: '#A37E2C',
                    backgroundColor: 'rgba(163, 126, 44, 0.1)',
                    borderDash: [5, 5],
                    tension: 0.4,
                    fill: false,
                    pointRadius: 3,
                    pointHoverRadius: 5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        font: {
                            family: 'Avenir, Arial, sans-serif',
                            size: 12
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleFont: {
                        family: 'Avenir, Arial, sans-serif'
                    },
                    bodyFont: {
                        family: 'Avenir, Arial, sans-serif'
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        font: {
                            family: 'Avenir, Arial, sans-serif'
                        }
                    }
                },
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        font: {
                            family: 'Avenir, Arial, sans-serif'
                        },
                        maxRotation: 45,
                        minRotation: 45
                    }
                }
            }
        }
    });
    
    charts[medium === 'social_media' ? 'socialMedia' : medium] = chart;
}

// ============================================================================
// TOP PERFORMERS
// ============================================================================

async function loadTopPerformers(medium, containerId) {
    try {
        const response = await fetch(
            `/api/customers/${currentCustomer.id}/top-performers?medium=${medium}&limit=3`
        );
        
        const data = await response.json();
        
        if (data.success) {
            renderTopPerformers(data.top_performers, containerId);
        }
    } catch (error) {
        console.error('Error loading top performers:', error);
    }
}

function renderTopPerformers(performers, containerId) {
    const container = document.getElementById(containerId);
    
    if (!performers || performers.length === 0) {
        container.innerHTML = '<p style="color: #757575; text-align: center; padding: 2rem;">No data available</p>';
        return;
    }
    
    container.innerHTML = '';
    
    performers.forEach((performer, index) => {
        const item = document.createElement('div');
        item.className = 'performer-item';
        
        item.innerHTML = `
            <span class="performer-rank">${index + 1}</span>
            <span class="performer-title">${performer.item_title}</span>
            <div class="performer-metric">
                <span class="performer-value">${Math.round(performer.metric_value).toLocaleString()}</span>
                ${performer.metric_name}
            </div>
        `;
        
        container.appendChild(item);
    });
}

// ============================================================================
// PDF EXPORT
// ============================================================================

async function exportToPDF() {
    if (!currentCustomer) {
        showToast('Please select a customer first', 'error');
        return;
    }
    
    showLoading(true);
    
    try {
        // Collect chart images
        const chartData = {};
        
        Object.keys(charts).forEach(key => {
            if (charts[key]) {
                chartData[key] = charts[key].toBase64Image();
            }
        });
        
        const response = await fetch(`/api/customers/${currentCustomer.id}/export/pdf`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ charts: chartData })
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `contentclicks_${currentCustomer.name.replace(/\s+/g, '_')}_${new Date().toISOString().split('T')[0]}.pdf`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            
            showToast('PDF exported successfully!', 'success');
        } else {
            showToast('Error exporting PDF', 'error');
        }
    } catch (error) {
        showToast('Error exporting PDF', 'error');
        console.error('Error:', error);
    } finally {
        showLoading(false);
    }
}

// ============================================================================
// UI UTILITIES
// ============================================================================

function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    container.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'slideIn 0.3s ease reverse';
        setTimeout(() => {
            container.removeChild(toast);
        }, 300);
    }, 3000);
}

// ============================================================================
// PAGE DISCOVERY & SELECTION
// ============================================================================

let discoveredPages = [];
let selectedPageIds = [];

function initializePageDiscovery() {
    const discoverBtn = document.getElementById('discoverPagesBtn');
    if (discoverBtn) {
        discoverBtn.addEventListener('click', discoverPages);
    }
}

async function discoverPages() {
    const systemToken = document.getElementById('systemUserToken').value;
    
    if (!systemToken) {
        showToast('Please enter a System User Token first', 'error');
        return;
    }
    
    const discoverBtn = document.getElementById('discoverPagesBtn');
    const pageSelectionArea = document.getElementById('pageSelectionArea');
    const availablePagesContainer = document.getElementById('availablePages');
    
    // Show loading
    discoverBtn.disabled = true;
    discoverBtn.textContent = 'ðŸ” Discovering...';
    availablePagesContainer.innerHTML = `
        <div class="loading-indicator">
            <div class="loading-spinner-small"></div>
            <p>Fetching your pages...</p>
        </div>
    `;
    pageSelectionArea.style.display = 'block';
    
    try {
        const response = await fetch('/api/discover-pages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ system_user_token: systemToken })
        });
        
        const data = await response.json();
        
        if (data.success && data.pages.length > 0) {
            discoveredPages = data.pages;
            renderPageSelection(data.pages);
            showToast(`Found ${data.pages.length} page(s)!`, 'success');
        } else if (data.success && data.pages.length === 0) {
            availablePagesContainer.innerHTML = `
                <div class="loading-indicator">
                    <p style="color: #DC2626;">No pages found. Check your token permissions.</p>
                </div>
            `;
            showToast('No pages found', 'error');
        } else {
            throw new Error(data.error || 'Failed to discover pages');
        }
        
    } catch (error) {
        console.error('Error discovering pages:', error);
        availablePagesContainer.innerHTML = `
            <div class="loading-indicator">
                <p style="color: #DC2626;">Error: ${error.message}</p>
                <p style="font-size: 12px;">Check that your token is valid and has page permissions.</p>
            </div>
        `;
        showToast('Failed to discover pages', 'error');
    } finally {
        discoverBtn.disabled = false;
        discoverBtn.textContent = 'ðŸ” Discover Available Pages';
    }
}

function renderPageSelection(pages) {
    const container = document.getElementById('availablePages');
    
    container.innerHTML = pages.map((page, index) => `
        <div class="page-item" data-page-id="${page.page_id}">
            <input type="checkbox" class="page-checkbox" id="page_${page.page_id}" checked>
            <div class="page-info">
                <div class="page-name">
                    ${page.page_name}
                    ${page.has_instagram ? '<span class="page-badge">+ INSTAGRAM</span>' : ''}
                </div>
                <div class="page-details">
                    Page ID: ${page.page_id}
                    ${page.has_instagram ? ` | Instagram ID: ${page.instagram_id}` : ''}
                    | ${page.fan_count.toLocaleString()} fans
                    ${page.has_instagram ? ` | ${page.followers_count.toLocaleString()} IG followers` : ''}
                </div>
            </div>
        </div>
    `).join('');
    
    // Auto-select all pages by default
    selectedPageIds = pages.map(p => p.page_id);
    
    // Add event listeners
    pages.forEach(page => {
        const checkbox = document.getElementById(`page_${page.page_id}`);
        const item = checkbox.closest('.page-item');
        item.classList.add('selected');
        
        checkbox.addEventListener('change', (e) => togglePageSelection(page.page_id, e.target.checked));
        item.addEventListener('click', (e) => {
            if (e.target !== checkbox) {
                checkbox.checked = !checkbox.checked;
                checkbox.dispatchEvent(new Event('change'));
            }
        });
    });
}

function togglePageSelection(pageId, selected) {
    const checkbox = document.getElementById(`page_${pageId}`);
    const item = checkbox.closest('.page-item');
    
    if (selected) {
        if (!selectedPageIds.includes(pageId)) {
            selectedPageIds.push(pageId);
        }
        item.classList.add('selected');
    } else {
        selectedPageIds = selectedPageIds.filter(id => id !== pageId);
        item.classList.remove('selected');
    }
}
