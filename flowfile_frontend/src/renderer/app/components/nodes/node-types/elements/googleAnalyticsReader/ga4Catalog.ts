// Curated catalog of common GA4 metrics and dimensions, grouped by category
// for the node-drawer pickers. Not exhaustive — users can still type a custom
// field name (e.g. ``customEvent:my_param``, ``customUser:...``) thanks to
// ``allow-create`` on the ElSelect component.
//
// Sourced from Google's public GA4 schema reference:
// https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema

export interface GaFieldOption {
  /** API name — this is what GA4's run_report expects. */
  name: string;
  /** Human-readable label shown in the dropdown. */
  label: string;
  /** Optional one-line hint shown after the label. */
  description?: string;
}

export interface GaFieldGroup {
  label: string;
  options: GaFieldOption[];
}

// ---------------------------------------------------------------------------
// Dimensions
// ---------------------------------------------------------------------------

export const GA4_DIMENSION_GROUPS: GaFieldGroup[] = [
  {
    label: "Page / screen",
    options: [
      { name: "pagePath", label: "Page path", description: "URL path without query string, e.g. /blog/hello" },
      { name: "pagePathPlusQueryString", label: "Page path + query" },
      { name: "pageLocation", label: "Page location", description: "Full URL" },
      { name: "pageTitle", label: "Page title" },
      { name: "pageReferrer", label: "Page referrer", description: "Referring URL" },
      { name: "hostName", label: "Hostname" },
      { name: "landingPage", label: "Landing page" },
      { name: "landingPagePlusQueryString", label: "Landing page + query" },
      { name: "screenName", label: "Screen name (mobile)" },
      { name: "unifiedPagePathScreen", label: "Page path or screen name" },
      { name: "contentGroup", label: "Content group" },
    ],
  },
  {
    label: "Event",
    options: [
      { name: "eventName", label: "Event name", description: "e.g. page_view, click, scroll" },
      { name: "isConversionEvent", label: "Is conversion event" },
      { name: "method", label: "Method" },
    ],
  },
  {
    label: "Links & files",
    options: [
      { name: "linkUrl", label: "Link URL", description: "Outbound link destination" },
      { name: "linkDomain", label: "Link domain" },
      { name: "linkText", label: "Link text" },
      { name: "linkClasses", label: "Link classes" },
      { name: "linkId", label: "Link ID" },
      { name: "outbound", label: "Outbound", description: "Whether the link left the site" },
      { name: "fileName", label: "File name" },
      { name: "fileExtension", label: "File extension" },
    ],
  },
  {
    label: "Video",
    options: [
      { name: "videoProvider", label: "Video provider" },
      { name: "videoTitle", label: "Video title" },
      { name: "videoUrl", label: "Video URL" },
      { name: "visible", label: "Visible (video/element)" },
    ],
  },
  {
    label: "Traffic source",
    options: [
      { name: "sessionSource", label: "Session source" },
      { name: "sessionMedium", label: "Session medium" },
      { name: "sessionCampaignName", label: "Session campaign" },
      { name: "sessionDefaultChannelGroup", label: "Session default channel group" },
      { name: "sessionSourceMedium", label: "Session source / medium" },
      { name: "firstUserSource", label: "First user source" },
      { name: "firstUserMedium", label: "First user medium" },
      { name: "firstUserCampaignName", label: "First user campaign" },
      { name: "firstUserDefaultChannelGroup", label: "First user channel group" },
      { name: "manualCampaignName", label: "Manual campaign name" },
      { name: "manualSource", label: "Manual source" },
      { name: "manualMedium", label: "Manual medium" },
    ],
  },
  {
    label: "Search",
    options: [
      { name: "searchTerm", label: "Search term", description: "Site search query" },
      { name: "googleAdsQuery", label: "Google Ads query" },
    ],
  },
  {
    label: "Geography",
    options: [
      { name: "country", label: "Country" },
      { name: "countryId", label: "Country ID" },
      { name: "region", label: "Region" },
      { name: "city", label: "City" },
      { name: "continent", label: "Continent" },
      { name: "subContinent", label: "Sub-continent" },
    ],
  },
  {
    label: "Device & platform",
    options: [
      { name: "deviceCategory", label: "Device category", description: "desktop / mobile / tablet" },
      { name: "deviceModel", label: "Device model" },
      { name: "operatingSystem", label: "Operating system" },
      { name: "operatingSystemVersion", label: "OS version" },
      { name: "browser", label: "Browser" },
      { name: "browserVersion", label: "Browser version" },
      { name: "language", label: "Language" },
      { name: "platform", label: "Platform", description: "web / iOS / android" },
      { name: "platformDeviceCategory", label: "Platform + device category" },
      { name: "screenResolution", label: "Screen resolution" },
    ],
  },
  {
    label: "Audience & user",
    options: [
      { name: "newVsReturning", label: "New vs returning" },
      { name: "userAgeBracket", label: "Age bracket" },
      { name: "userGender", label: "Gender" },
      { name: "audienceName", label: "Audience name" },
      { name: "cohort", label: "Cohort" },
      { name: "cohortNthDay", label: "Cohort day" },
      { name: "cohortNthWeek", label: "Cohort week" },
      { name: "cohortNthMonth", label: "Cohort month" },
    ],
  },
  {
    label: "Time",
    options: [
      { name: "date", label: "Date", description: "YYYYMMDD → Polars Date" },
      { name: "dateHour", label: "Date + hour" },
      { name: "dateHourMinute", label: "Date + hour + minute" },
      { name: "year", label: "Year" },
      { name: "yearMonth", label: "Year + month" },
      { name: "yearWeek", label: "Year + week" },
      { name: "month", label: "Month" },
      { name: "week", label: "Week" },
      { name: "day", label: "Day" },
      { name: "dayOfWeek", label: "Day of week" },
      { name: "dayOfWeekName", label: "Day of week (name)" },
      { name: "hour", label: "Hour" },
      { name: "minute", label: "Minute" },
      { name: "nthDay", label: "Nth day" },
      { name: "nthHour", label: "Nth hour" },
      { name: "nthMinute", label: "Nth minute" },
      { name: "nthMonth", label: "Nth month" },
      { name: "nthWeek", label: "Nth week" },
      { name: "nthYear", label: "Nth year" },
    ],
  },
  {
    label: "E-commerce",
    options: [
      { name: "itemName", label: "Item name" },
      { name: "itemId", label: "Item ID" },
      { name: "itemBrand", label: "Item brand" },
      { name: "itemCategory", label: "Item category" },
      { name: "itemListName", label: "Item list name" },
      { name: "itemVariant", label: "Item variant" },
      { name: "transactionId", label: "Transaction ID" },
      { name: "currencyCode", label: "Currency code" },
      { name: "paymentType", label: "Payment type" },
    ],
  },
  {
    label: "App",
    options: [
      { name: "appVersion", label: "App version" },
      { name: "streamId", label: "Stream ID" },
      { name: "streamName", label: "Stream name" },
    ],
  },
];

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

export const GA4_METRIC_GROUPS: GaFieldGroup[] = [
  {
    label: "Users",
    options: [
      { name: "activeUsers", label: "Active users" },
      { name: "newUsers", label: "New users" },
      { name: "totalUsers", label: "Total users" },
      { name: "dauPerMau", label: "DAU / MAU" },
      { name: "wauPerMau", label: "WAU / MAU" },
      { name: "dauPerWau", label: "DAU / WAU" },
      { name: "engagedSessionsPerActiveUser", label: "Engaged sessions per active user" },
    ],
  },
  {
    label: "Sessions & engagement",
    options: [
      { name: "sessions", label: "Sessions" },
      { name: "engagedSessions", label: "Engaged sessions" },
      { name: "engagementRate", label: "Engagement rate" },
      { name: "bounces", label: "Bounces" },
      { name: "bounceRate", label: "Bounce rate" },
      { name: "averageSessionDuration", label: "Avg. session duration (s)" },
      { name: "sessionsPerUser", label: "Sessions per user" },
      { name: "userEngagementDuration", label: "User engagement duration (s)" },
    ],
  },
  {
    label: "Page & screen views",
    options: [
      { name: "screenPageViews", label: "Page / screen views" },
      { name: "screenPageViewsPerSession", label: "Views per session" },
      { name: "screenPageViewsPerUser", label: "Views per user" },
      { name: "scrolledUsers", label: "Scrolled users" },
    ],
  },
  {
    label: "Events",
    options: [
      { name: "eventCount", label: "Event count" },
      { name: "eventCountPerUser", label: "Event count per user" },
      { name: "eventValue", label: "Event value" },
      { name: "eventsPerSession", label: "Events per session" },
      { name: "keyEvents", label: "Key events (conversions)" },
    ],
  },
  {
    label: "Search",
    options: [
      { name: "organicGoogleSearchClicks", label: "Organic Google search clicks" },
      { name: "organicGoogleSearchImpressions", label: "Organic Google search impressions" },
      { name: "organicGoogleSearchClickThroughRate", label: "Organic search CTR" },
      { name: "organicGoogleSearchAveragePosition", label: "Organic search avg. position" },
    ],
  },
  {
    label: "E-commerce",
    options: [
      { name: "transactions", label: "Transactions" },
      { name: "totalRevenue", label: "Total revenue" },
      { name: "purchaseRevenue", label: "Purchase revenue" },
      { name: "averagePurchaseRevenue", label: "Avg. purchase revenue" },
      { name: "averagePurchaseRevenuePerUser", label: "Avg. revenue per user" },
      { name: "transactionsPerPurchaser", label: "Transactions per purchaser" },
      { name: "addToCarts", label: "Add-to-carts" },
      { name: "checkouts", label: "Checkouts" },
      { name: "itemsAddedToCart", label: "Items added to cart" },
      { name: "itemsCheckedOut", label: "Items checked out" },
      { name: "itemsPurchased", label: "Items purchased" },
      { name: "itemsViewed", label: "Items viewed" },
      { name: "itemViewEvents", label: "Item view events" },
      { name: "cartToViewRate", label: "Cart-to-view rate" },
      { name: "purchaserRate", label: "Purchaser rate" },
      { name: "firstTimePurchaserRate", label: "First-time purchaser rate" },
      { name: "purchaseToViewRate", label: "Purchase-to-view rate" },
      { name: "refundAmount", label: "Refund amount" },
    ],
  },
  {
    label: "Advertising",
    options: [
      { name: "advertiserAdCost", label: "Ad cost" },
      { name: "advertiserAdClicks", label: "Ad clicks" },
      { name: "advertiserAdImpressions", label: "Ad impressions" },
      { name: "advertiserAdCostPerClick", label: "Cost per click (CPC)" },
      { name: "advertiserAdCostPerKeyEvent", label: "Cost per key event" },
      { name: "returnOnAdSpend", label: "Return on ad spend (ROAS)" },
    ],
  },
  {
    label: "Predictive",
    options: [
      { name: "purchaseProbability", label: "Purchase probability" },
      { name: "churnProbability", label: "Churn probability" },
      { name: "predictedRevenue", label: "Predicted 28-day revenue" },
    ],
  },
];

// Flat helpers used by the picker to resolve a name -> label when the user has
// previously picked a field (so chips render with the friendly label if we
// know it).
const _flattenGroups = (groups: GaFieldGroup[]): Record<string, GaFieldOption> => {
  const out: Record<string, GaFieldOption> = {};
  for (const group of groups) for (const option of group.options) out[option.name] = option;
  return out;
};

export const GA4_DIMENSION_INDEX = _flattenGroups(GA4_DIMENSION_GROUPS);
export const GA4_METRIC_INDEX = _flattenGroups(GA4_METRIC_GROUPS);

export const getDimensionLabel = (name: string): string =>
  GA4_DIMENSION_INDEX[name]?.label ?? name;

export const getMetricLabel = (name: string): string => GA4_METRIC_INDEX[name]?.label ?? name;
