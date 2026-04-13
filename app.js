"use strict";

// Configure this with your deployed Cloudflare Worker URL (no token in client code).
const IMPROVE_UPLOAD_ENDPOINT = "https://broad-star-66c2.mrtnhliza.workers.dev/";
const IMPROVE_UPLOAD_TIMEOUT_MS = 45000;
const IMPROVE_UPLOAD_MAX_FILE_BYTES = 50 * 1024 * 1024;
const IMPROVE_UPLOAD_UNEXPECTED_MAX_FILE_BYTES = 50 * 1024 * 1024;
const IMPROVE_UPLOAD_UNEXPECTED_TOTAL_LIMIT_BYTES = 1024 * 1024 * 1024;
const IMPROVE_UPLOAD_UNEXPECTED_MAX_WHEN_TOTAL_EXCEEDED = 3;

const BASE_MAPS = [
  { code: "US_01", name: "Michigan" },
  { code: "US_02", name: "Alaska" },
  { code: "RU_02", name: "Taymyr" },
];

const SEASON_REGION_MAP = {
  1: { code: "RU_03", label: "Season 1: Search & Recover (Kola Peninsula)" },
  2: { code: "US_04", label: "Season 2: Explore & Expand (Yukon)" },
  3: { code: "US_03", label: "Season 3: Locate & Deliver (Wisconsin)" },
  4: { code: "RU_04", label: "Season 4: New Frontiers (Amur)" },
  5: { code: "RU_05", label: "Season 5: Build & Dispatch (Don)" },
  6: { code: "US_06", label: "Season 6: Haul & Hustle (Maine)" },
  7: { code: "US_07", label: "Season 7: Compete & Conquer (Tennessee)" },
  8: { code: "RU_08", label: "Season 8: Grand Harvest (Glades)" },
  9: { code: "US_09", label: "Season 9: Renew & Rebuild (Ontario)" },
  10: { code: "US_10", label: "Season 10: Fix & Connect (British Columbia)" },
  11: { code: "US_11", label: "Season 11: Lights & Cameras (Scandinavia)" },
  12: { code: "US_12", label: "Season 12: Public Energy (North Carolina)" },
  13: { code: "RU_13", label: "Season 13: Dig & Drill (Almaty)" },
  14: { code: "US_14", label: "Season 14: Reap & Sow (Austria)" },
  15: { code: "US_15", label: "Season 15: Oil & Dirt (Quebec)" },
  16: { code: "US_16", label: "Season 16: High Voltage (Washington)" },
  17: { code: "RU_17", label: "Season 17: Repair & Rescue (Zurdania)" },
};

const SEASON_ID_MAP = Object.fromEntries(
  Object.entries(SEASON_REGION_MAP).map(([season, data]) => [Number(season), data.code]),
);

const REGION_LONG_NAME_MAP = (() => {
  const out = {};
  for (const map of BASE_MAPS) {
    out[map.code.toUpperCase()] = map.name;
  }
  for (const season of Object.keys(SEASON_REGION_MAP).map(Number).sort((a, b) => a - b)) {
    const entry = SEASON_REGION_MAP[season];
    if (!entry) {
      continue;
    }
    out[String(entry.code || "").toUpperCase()] = entry.label || entry.code;
  }
  out.TRIALS = "Trials";
  return out;
})();

const REGION_ORDER = Object.keys(REGION_LONG_NAME_MAP);

const RANK_XP_REQUIREMENTS = {
  1: 0,
  2: 700,
  3: 1700,
  4: 2900,
  5: 4100,
  6: 5400,
  7: 6900,
  8: 8500,
  9: 10100,
  10: 11800,
  11: 13700,
  12: 15700,
  13: 17800,
  14: 20100,
  15: 22500,
  16: 25000,
  17: 27500,
  18: 30100,
  19: 32700,
  20: 35500,
  21: 38300,
  22: 41300,
  23: 44300,
  24: 47500,
  25: 50700,
  26: 54100,
  27: 57500,
  28: 61100,
  29: 64900,
  30: 69000,
};

const MONEY_MIN = -2147483647;
const MONEY_MAX = 2147483647;
const SAVE_VERSION_EXPECTED = {
  objVersion: 9,
  birthVersion: 9,
  cfg_version: 1,
};
const DEFAULT_RECOVERY_PRICE = [0, 0, 2500, 5000, 8000, 5000, 2000];
const DEFAULT_FULL_REPAIR_PRICE = [0, 0, 1500, 2500, 5000, 2500, 1500];
const DEFAULT_SETTINGS_DICT = {
  ADDON_AVAILABILITY: 1,
  CONTEST_ATTEMPTS: 0,
  STARTING_MONEY: 0,
  REPAIR_POINTS_AMOUNT: 0,
  TRUCK_SELLING: 0,
  MAP_MARKER: 0,
  TRAILER_AVAILABILITY: 1,
  RECOVERY: 0,
  TIME_SETTINGS: 0,
  STARTING_RANK: 0,
  GARAGE_REPAIRE: 0,
  TYRE_AVAILABILITY: 1,
  REPAIR_POINTS_COST: 0,
  TRUCK_AVAILABILITY: 3,
  REGION_TRAVELLING: 0,
  VEHICLE_STORAGE: 0,
  LOADING: 0,
  FUEL_PRICE: 1,
  STARTING_RULES: 0,
  INTENAL_ADDON_AVAILABILITY: 1,
  TASKS_CONTESTS: 0,
  GARAGE_REFUEL: 0,
  TRAILER_STORE_AVAILBILITY: 0,
  DLC_VEHICLES: 1,
  TELEPORTATION: 0,
  CONTRACTS: 0,
  TRAILER_PRICING: 0,
  TRUCK_PRICING: 0,
  TRAILER_SELLING: 0,
  VEHICLE_DAMAGE: 0,
  ADDON_PRICING: 0,
  REGIONAL_REPAIR: 0,
};
const DEFAULT_DEPLOY_PRICE = { Region: 3500, Map: 1000 };
const DEFAULT_AUTOLOAD_PRICE = 150;
const SAFE_DEFAULTS = (() => {
  const source =
    typeof window !== "undefined" && window.SR_EDITOR_DEFAULTS && typeof window.SR_EDITOR_DEFAULTS === "object"
      ? window.SR_EDITOR_DEFAULTS
      : {};
  return {
    upgradesGiverUnlocks:
      source.upgradesGiverUnlocks && typeof source.upgradesGiverUnlocks === "object"
        ? source.upgradesGiverUnlocks
        : {},
    watchpointsUnlocks:
      source.watchpointsUnlocks && typeof source.watchpointsUnlocks === "object"
        ? source.watchpointsUnlocks
        : {},
    discoveredTrucksDefaults:
      source.discoveredTrucksDefaults && typeof source.discoveredTrucksDefaults === "object"
        ? source.discoveredTrucksDefaults
        : {},
  };
})();

const TRIALS_LIST = [
  ["Ride-on King", "TRIAL_01_01_SCOUTING_CNT"],
  ["Lost in wilderness", "TRIAL_01_02_TRUCK_TSK"],
  ["Snowbound Valley", "TRIAL_02_01_DELIVERING"],
  ["Zalukodes", "TRIAL_02_02_SEARCH_CNT"],
  ["Northern Thread", "TRIAL_03_01_SCOUTING_CNT"],
  ["Wolves' Bog", "TRIAL_03_03_SCOUTING_CNT"],
  ["The Slope", "TRIAL_04_02_TSK"],
  ["Escape from Tretyakov", "TRIAL_04_01_SCOUTING_CNT"],
  ["Aftermath", "TRIAL_05_01_TSK"],
  ["Tumannaya Pass", "TRIAL_03_02_DELIVERY_CNT"],
];

const PROS_ENTITLEMENTS = [
  ["ProsRegistrationReward", "Mammoth Ornament & Stickers"],
  ["ProsRoadcraftReward", "Voron-AE4380 skin + 3 stickers"],
];

const VISITED_LEVELS_DEFAULTS = [
  "level_ru_02_01_crop",
  "level_ru_02_02",
  "level_ru_02_03",
  "level_ru_02_04",
  "level_ru_03_01",
  "level_ru_03_02",
  "level_ru_04_01",
  "level_ru_04_02",
  "level_ru_04_03",
  "level_ru_04_04",
  "level_ru_05_01",
  "level_ru_05_02",
  "level_ru_08_01",
  "level_ru_08_02",
  "level_ru_08_03",
  "level_ru_08_04",
  "level_ru_13_01",
  "level_ru_17_01",
  "level_ru_17_02",
  "level_us_01_01",
  "level_us_01_02",
  "level_us_01_03",
  "level_us_01_04_new",
  "level_us_02_01",
  "level_us_02_02_new",
  "level_us_02_03_new",
  "level_us_02_04_new",
  "level_us_03_01",
  "level_us_03_02",
  "level_us_04_01",
  "level_us_04_02",
  "level_us_06_01",
  "level_us_06_02",
  "level_us_07_01",
  "level_us_09_01",
  "level_us_09_02",
  "level_us_10_01",
  "level_us_10_02",
  "level_us_11_01",
  "level_us_11_02",
  "level_us_12_01",
  "level_us_12_02",
  "level_us_12_03",
  "level_us_12_04",
  "level_us_14_01",
  "level_us_14_02",
  "level_us_15_01",
  "level_us_15_02",
  "level_us_16_01",
  "level_us_16_02",
  "level_us_16_03",
];

const LEVEL_GARAGE_STATUSES_DEFAULTS = {
  level_us_12_02: 1,
  level_ru_03_01: 2,
  level_ru_04_01: 2,
  level_ru_08_03: 1,
  level_us_04_01: 2,
  level_us_03_01: 2,
  level_ru_05_01: 2,
  level_us_02_01: 2,
  level_ru_02_04: 0,
  level_us_01_02: 1,
  level_us_11_02: 0,
  level_us_14_01: 2,
  level_ru_03_02: 1,
  level_us_09_02: 0,
  level_ru_02_01_crop: 0,
  level_ru_02_02: 2,
  level_ru_17_01: 2,
  level_us_01_01: 2,
  level_ru_08_04: 1,
  level_us_15_01: 2,
  level_us_02_03_new: 1,
  level_us_01_03: 0,
  level_us_12_03: 1,
  level_us_14_02: 0,
  level_us_16_01: 0,
  level_ru_08_02: 0,
  level_us_16_03: 0,
  level_ru_05_02: 0,
  level_us_12_04: 0,
  level_us_10_01: 2,
  level_ru_17_02: 0,
  level_us_06_01: 2,
  level_us_02_02_new: 0,
  level_us_04_02: 1,
  level_us_16_02: 2,
  level_us_10_02: 1,
  level_us_01_04_new: 0,
  level_us_06_02: 0,
  level_ru_04_04: 1,
  level_us_02_04_new: 0,
  level_ru_02_03: 1,
  level_ru_04_02: 1,
  level_us_03_02: 1,
  level_us_15_02: 0,
  level_us_11_01: 2,
  level_ru_04_03: 0,
  level_ru_08_01: 2,
  level_us_07_01: 2,
  level_ru_13_01: 2,
  level_us_12_01: 2,
  level_us_09_01: 2,
};

const REGION_LEVELS = (() => {
  const out = {};
  for (const levelId of VISITED_LEVELS_DEFAULTS) {
    const m = /^level_([a-z]{2}_\d{2})/i.exec(levelId);
    if (!m) {
      continue;
    }
    const code = m[1].toUpperCase();
    if (!out[code]) {
      out[code] = [];
    }
    out[code].push(levelId);
  }
  return out;
})();

const RULE_RANDOM_LABEL = "random";
const RULE_RANDOM_VALUE = "__RULE_RANDOM__";

const RULE_DEFINITIONS = [
  {
    label: "Game difficulty",
    key: "gameDifficultyMode",
    options: [
      { label: "Normal", value: 0 },
      { label: "Hard", value: 1 },
      { label: "New Game+", value: 2 },
    ],
  },
  {
    label: "Truck availability",
    key: "truckAvailability",
    options: [
      { label: "default", value: 1 },
      { label: "all trucks available from start", value: 0 },
      { label: "5-15 trucks in each garage", value: 3 },
      { label: "store unlocks at rank 10", value: 2 },
      { label: "store unlocks at rank 20", value: 2 },
      { label: "store unlocks at rank 30", value: 2 },
      { label: "store is locked", value: 4 },
    ],
  },
  {
    label: "Truck pricing",
    key: "truckPricingFactor",
    options: [
      { label: "default", value: 1 },
      { label: "free", value: 0 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Truck selling price",
    key: "truckSellingFactor",
    options: [
      { label: "normal price", value: 1 },
      { label: "50%", value: 0.5 },
      { label: "30%", value: 0.3 },
      { label: "10%", value: 0.1 },
      { label: "cant be sold", value: -1 },
    ],
  },
  {
    label: "DLC vehicles availability",
    key: "needToAddDlcTrucks",
    options: [
      { label: "available", value: true },
      { label: "unavailable", value: false },
    ],
  },
  {
    label: "Vehicle storage slots",
    key: "vehicleStorageSlots",
    options: [
      { label: "default", value: 0 },
      { label: "only 3", value: 3 },
      { label: "only 5", value: 5 },
      { label: "only 10", value: 10 },
      { label: "only scouts", value: -1 },
    ],
  },
  {
    label: "External addon availability",
    key: "externalAddonAvailability",
    options: [
      { label: "default", value: 0 },
      { label: "all addons unlocked", value: 1 },
    ],
  },
  {
    label: "Internal addon availability",
    key: "internalAddonAvailability",
    options: [
      { label: "default", value: 0 },
      { label: "all internal addons unlocked", value: 1 },
      { label: "10-50 per garage", value: 2 },
      { label: "30-100 per garage", value: 2 },
      { label: "50-150 per garage", value: 2 },
      { label: "0-100 per garage", value: 2 },
    ],
  },
  {
    label: "Tire availability",
    key: "tyreAvailability",
    options: [
      { label: "default", value: 1 },
      { label: "all tires available", value: 0 },
      { label: "highway and allroad", value: 2 },
      { label: "highway, allroad, offroad", value: 3 },
      { label: "no mud tires", value: 4 },
      { label: "no chained tires", value: 5 },
    ],
  },
  {
    label: "Vehicle addon pricing",
    key: "addonPricingFactor",
    options: [
      { label: "default", value: 1 },
      { label: "free", value: 0 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Addon selling price",
    key: "addonSellingFactor",
    options: [
      { label: "normal", value: 1.0 },
      { label: "10%", value: 0.1 },
      { label: "30%", value: 0.3 },
      { label: "50%", value: 0.5 },
      { label: "no refunds", value: 0 },
    ],
  },
  {
    label: "Trailer store availability",
    key: "trailerStoreAviability",
    options: [
      { label: "default", value: 0 },
      { label: "one random store per region", value: 1 },
    ],
  },
  {
    label: "Trailer availability",
    key: "trailerAvailability",
    options: [
      { label: "default", value: 0 },
      { label: "all trailers available", value: 1 },
    ],
  },
  {
    label: "Trailer pricing",
    key: "trailerPricingFactor",
    options: [
      { label: "normal price", value: 1 },
      { label: "free", value: 0 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Trailer selling price",
    key: "trailerSellingFactor",
    options: [
      { label: "normal price", value: 1 },
      { label: "50%", value: 0.5 },
      { label: "30%", value: 0.3 },
      { label: "10%", value: 0.1 },
      { label: "cant be sold", value: -1 },
    ],
  },
  {
    label: "Fuel price",
    key: "fuelPriceFactor",
    options: [
      { label: "default", value: 0 },
      { label: "free", value: 0 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Garage repair price",
    key: "garageRepairePriceFactor",
    options: [
      { label: "default", value: 0 },
      { label: "paid", value: 1 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Garage refuelling",
    key: "isGarageRefuelAvailable",
    options: [
      { label: "available", value: true },
      { label: "unavailable", value: false },
    ],
  },
  {
    label: "Repair points cost",
    key: "repairPointsCostFactor",
    options: [
      { label: "default", value: 1 },
      { label: "paid", value: 1 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Repair points required",
    key: "repairPointsRequiredFactor",
    options: [
      { label: "default", value: 1 },
      { label: "2x more effective", value: 0.5 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Vehicle repair regional rules",
    key: "regionRepaireMoneyFactor",
    options: [
      { label: "default", value: 1 },
      { label: "2x outside home region", value: 2 },
      { label: "3x outside home region", value: 3 },
      { label: "4x outside home region", value: 4 },
    ],
  },
  {
    label: "Vehicle damage",
    key: "vehicleDamageFactor",
    options: [
      { label: "default", value: 1 },
      { label: "no damage", value: 0 },
      { label: "2x", value: 2 },
      { label: "3x", value: 3 },
      { label: "5x", value: 5 },
    ],
  },
  {
    label: "Recovery price",
    key: "recoveryPriceFactor",
    options: [
      { label: "default", value: 0 },
      { label: "paid", value: 1 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
      { label: "unavailable", value: -1 },
    ],
  },
  {
    label: "Automatic cargo loading",
    key: "loadingPriceFactor",
    options: [
      { label: "default", value: 0 },
      { label: "paid", value: 1 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Truck switching price (minimap)",
    key: "teleportationPrice",
    options: [
      { label: "free", value: 0 },
      { label: "500", value: 500 },
      { label: "1000", value: 1000 },
      { label: "2000", value: 2000 },
      { label: "5000", value: 5000 },
    ],
  },
  {
    label: "Region traveling price",
    key: "regionTravellingPriceFactor",
    options: [
      { label: "default", value: 0 },
      { label: "paid", value: 1 },
      { label: "2x", value: 2 },
      { label: "4x", value: 4 },
      { label: "6x", value: 6 },
    ],
  },
  {
    label: "Task and contest payouts",
    key: "tasksAndContestsPayoutsFactor",
    options: [
      { label: "normal", value: 1 },
      { label: "50%", value: 0.5 },
      { label: "150%", value: 1.5 },
      { label: "200%", value: 2 },
      { label: "300%", value: 3 },
    ],
  },
  {
    label: "Contracts payouts",
    key: "contractsPayoutsFactor",
    options: [
      { label: "normal", value: 1 },
      { label: "50%", value: 0.5 },
      { label: "150%", value: 1.5 },
      { label: "200%", value: 2 },
      { label: "300%", value: 3 },
    ],
  },
  {
    label: "Max contest attempts",
    key: "maxContestAttempts",
    options: [
      { label: "default", value: 0 },
      { label: "1 attempt", value: 1 },
      { label: "3 attempts", value: 3 },
      { label: "5 attempts", value: 5 },
      { label: "gold time only", value: -1 },
    ],
  },
  {
    label: "Map marker style",
    key: "isMapMarkerAsInHardMode",
    options: [
      { label: "default", value: false },
      { label: "hard mode", value: true },
    ],
  },
];

const INTERNAL_ADDON_AMOUNT_BY_LABEL = {
  "10-50 per garage": 30,
  "30-100 per garage": 65,
  "50-150 per garage": 100,
  "0-100 per garage": 50,
};

const RULE_NGP_DICT_META = {
  truckAvailability: {
    ngpKey: "TRUCK_AVAILABILITY",
    labelToState: {
      default: 0,
      "all trucks available from start": 1,
      "5-15 trucks in each garage": 2,
      "store unlocks at rank 10": 3,
      "store unlocks at rank 20": 4,
      "store unlocks at rank 30": 5,
      "store is locked": 6,
    },
  },
  truckPricingFactor: { ngpKey: "TRUCK_PRICING", labelToState: { default: 0, free: 1, "2x": 2, "4x": 3, "6x": 4 } },
  truckSellingFactor: { ngpKey: "TRUCK_SELLING", labelToState: { "normal price": 0, "50%": 1, "30%": 2, "10%": 3, "cant be sold": 4 } },
  needToAddDlcTrucks: { ngpKey: "DLC_VEHICLES", labelToState: { available: 0, unavailable: 1 } },
  vehicleStorageSlots: { ngpKey: "VEHICLE_STORAGE", labelToState: { default: 0, "only 3": 1, "only 5": 2, "only 10": 3, "only scouts": 4 } },
  externalAddonAvailability: { ngpKey: "ADDON_AVAILABILITY", labelToState: { default: 0, "all addons unlocked": 1 } },
  internalAddonAvailability: {
    ngpKey: "INTENAL_ADDON_AVAILABILITY",
    labelToState: {
      default: 0,
      "all internal addons unlocked": 1,
      "10-50 per garage": 2,
      "30-100 per garage": 3,
      "50-150 per garage": 4,
      "0-100 per garage": 5,
    },
  },
  tyreAvailability: {
    ngpKey: "TYRE_AVAILABILITY",
    labelToState: {
      default: 0,
      "all tires available": 1,
      "highway and allroad": 2,
      "highway, allroad, offroad": 3,
      "no mud tires": 4,
      "no chained tires": 5,
    },
  },
  trailerStoreAviability: { ngpKey: "TRAILER_STORE_AVAILBILITY", labelToState: { default: 0, "one random store per region": 1 } },
  trailerAvailability: { ngpKey: "TRAILER_AVAILABILITY", labelToState: { default: 0, "all trailers available": 1 } },
  trailerPricingFactor: { ngpKey: "TRAILER_PRICING", labelToState: { "normal price": 0, free: 1, "2x": 2, "4x": 3, "6x": 4 } },
  trailerSellingFactor: { ngpKey: "TRAILER_SELLING", labelToState: { "normal price": 0, "50%": 1, "30%": 2, "10%": 3, "cant be sold": 4 } },
  fuelPriceFactor: { ngpKey: "FUEL_PRICE", labelToState: { default: 0, free: 1, "2x": 2, "4x": 3, "6x": 4 } },
  garageRepairePriceFactor: { ngpKey: "GARAGE_REPAIRE", labelToState: { default: 0, paid: 1, "2x": 2, "4x": 3, "6x": 4 } },
  isGarageRefuelAvailable: { ngpKey: "GARAGE_REFUEL", labelToState: { available: 0, unavailable: 1 } },
  repairPointsCostFactor: { ngpKey: "REPAIR_POINTS_COST", labelToState: { default: 0, paid: 1, "2x": 2, "4x": 3, "6x": 4 } },
  repairPointsRequiredFactor: { ngpKey: "REPAIR_POINTS_AMOUNT", labelToState: { default: 0, "2x more effective": 1, "2x": 2, "4x": 3, "6x": 4 } },
  regionRepaireMoneyFactor: {
    ngpKey: "REGIONAL_REPAIR",
    labelToState: {
      default: 0,
      "2x outside home region": 1,
      "3x outside home region": 2,
      "4x outside home region": 3,
    },
  },
  vehicleDamageFactor: { ngpKey: "VEHICLE_DAMAGE", labelToState: { default: 0, "no damage": 1, "2x": 2, "3x": 3, "5x": 4 } },
  recoveryPriceFactor: { ngpKey: "RECOVERY", labelToState: { default: 0, paid: 1, "2x": 2, "4x": 3, "6x": 4, unavailable: 5 } },
  loadingPriceFactor: { ngpKey: "LOADING", labelToState: { default: 0, paid: 1, "2x": 2, "4x": 3, "6x": 4 } },
  teleportationPrice: { ngpKey: "TELEPORTATION", labelToState: { free: 0, "500": 1, "1000": 2, "2000": 3, "5000": 4 } },
  regionTravellingPriceFactor: { ngpKey: "REGION_TRAVELLING", labelToState: { default: 0, paid: 1, "2x": 2, "4x": 3, "6x": 4 } },
  tasksAndContestsPayoutsFactor: { ngpKey: "TASKS_CONTESTS", labelToState: { normal: 0, "50%": 1, "150%": 2, "200%": 3, "300%": 4 } },
  contractsPayoutsFactor: { ngpKey: "CONTRACTS", labelToState: { normal: 0, "50%": 1, "150%": 2, "200%": 3, "300%": 4 } },
  maxContestAttempts: { ngpKey: "CONTEST_ATTEMPTS", labelToState: { default: 0, "1 attempt": 1, "3 attempts": 2, "5 attempts": 3, "gold time only": 4 } },
  isMapMarkerAsInHardMode: { ngpKey: "MAP_MARKER", labelToState: { default: 0, "hard mode": 1 } },
  addonPricingFactor: { ngpKey: "ADDON_PRICING", labelToState: { default: 0, free: 1, "2x": 2, "4x": 3, "6x": 4 } },
};

const OBJECTIVES_CATALOG_CSV_URL = "./assets/objectives_catalog.csv";
const OBJECTIVES_CATALOG_SCRIPT_URL = "./assets/objectives_catalog.js?v=20260226-1";
const EMBEDDED_OBJECTIVES_CSV = "";
let objectivesCatalogScriptPromise = null;
const GA_MEASUREMENT_ID = "G-1MZVZLT3SX";
const VENDOR_SCRIPT_URLS = {
  pako: "./vendor/pako.min.js?v=20260226-1",
  jszip: "./vendor/jszip.min.js?v=20260226-1",
};
const vendorLoadPromises = {
  pako: null,
  jszip: null,
};

const state = {
  main: null,
  common: null,
  selectors: {
    missions: null,
    contests: null,
    regions: null,
    upgrades: null,
    watchtowers: null,
    discoveries: null,
    levels: null,
    garages: null,
  },
  gameStats: {
    distanceInputs: new Map(),
    statInputs: new Map(),
  },
  ui: {
    rankXpSyncLock: false,
    showLegacyRegionActions: false,
  },
  objectives: {
    data: null,
    selected: new Set(),
    visibleKeys: [],
    finishedInSave: new Set(),
    catalogKeys: new Set(),
    catalogNames: {},
    catalogMeta: {},
    catalogSource: "",
    catalogLoaded: false,
    catalogLoading: false,
    catalogWarmupScheduled: false,
    catalogLoadPromise: null,
  },
  rules: {
    controls: new Map(),
  },
  regions: {
    featureControls: new Map(),
  },
  folder: {
    loaded: false,
    rootName: "",
    sourceKind: "standard",
    files: new Map(),
    rawFiles: new Map(),
    wgsBlobByLogicalKey: new Map(),
  },
  improveShare: {
    uploading: false,
    lastUploadedSignature: "",
    preferredSource: "",
  },
  fog: {
    brushValue: 255,
    brushSize: 8,
    currentKey: "",
    currentName: "",
    currentBytes: null,
    currentPixels: null,
    currentWidth: 0,
    currentHeight: 0,
    currentFooter: new Uint8Array(0),
    currentZlibOffset: 0,
    currentZlibLength: 0,
    currentZlibHeader: new Uint8Array([0x78, 0x9c]),
    drawing: false,
    lastX: 0,
    lastY: 0,
    viewScale: 1,
    viewOffsetX: 0,
    viewOffsetY: 0,
    seasonChecks: new Map(),
  },
  trialChecks: new Map(),
};

const els = {
  status: document.getElementById("status"),
  mainInput: document.getElementById("main-file-input"),
  commonInput: document.getElementById("common-file-input"),
  singleInput: document.getElementById("single-file-input"),
  folderInput: document.getElementById("folder-input"),
  folderUploadBtn: document.getElementById("folder-upload-btn"),
  singleUploadBtn: document.getElementById("single-upload-btn"),
  improveShareCheckbox: document.getElementById("improve-share-checkbox"),
  improveShareMeta: document.getElementById("improve-share-meta"),
  mainMeta: document.getElementById("main-meta"),
  commonMeta: document.getElementById("common-meta"),
  folderMeta: document.getElementById("folder-meta"),
  folderMainChoices: document.getElementById("folder-main-choices"),
  downloadMainBtn: document.getElementById("download-main-btn"),
  downloadCommonBtn: document.getElementById("download-common-btn"),
  downloadFolderBtn: document.getElementById("download-folder-btn"),
  tabNav: document.getElementById("tab-nav"),
  refreshGameStatsBtn: document.getElementById("refresh-game-stats-btn"),
  saveGameStatsBtn: document.getElementById("save-game-stats-btn"),
  gameStatsInfo: document.getElementById("game-stats-info"),
  distanceList: document.getElementById("distance-list"),
  gameStatList: document.getElementById("game-stat-list"),
  moneyInput: document.getElementById("money-input"),
  rankInput: document.getElementById("rank-input"),
  xpInput: document.getElementById("xp-input"),
  rankXpHelpBtn: document.getElementById("rank-xp-help-btn"),
  rankXpTable: document.getElementById("rank-xp-table"),
  timeDayInput: document.getElementById("time-day-input"),
  timeNightInput: document.getElementById("time-night-input"),
  skipTimeInput: document.getElementById("skip-time-input"),
  applyMoneyRankBtn: document.getElementById("apply-money-rank-btn"),
  applyTimeBtn: document.getElementById("apply-time-btn"),
  applyMissionsBtn: document.getElementById("apply-missions-btn"),
  applyContestsBtn: document.getElementById("apply-contests-btn"),
  objectivesSearch: document.getElementById("objectives-search"),
  objectivesType: document.getElementById("objectives-type"),
  objectivesRegion: document.getElementById("objectives-region"),
  objectivesCategory: document.getElementById("objectives-category"),
  refreshObjectivesBtn: document.getElementById("refresh-objectives-btn"),
  selectObjectivesBtn: document.getElementById("select-objectives-btn"),
  clearObjectivesBtn: document.getElementById("clear-objectives-btn"),
  completeObjectivesBtn: document.getElementById("complete-objectives-btn"),
  objectivesInfo: document.getElementById("objectives-info"),
  objectivesList: document.getElementById("objectives-list"),
  regionsCheckAllBtn: document.getElementById("regions-check-all-btn"),
  regionsClearBtn: document.getElementById("regions-clear-btn"),
  regionsFeatureList: document.getElementById("regions-feature-list"),
  regionsInfo: document.getElementById("regions-info"),
  regionsShowLegacy: document.getElementById("regions-show-legacy"),
  applyRegionsBtn: document.getElementById("apply-regions-btn"),
  unlockWatchBtn: document.getElementById("unlock-watchtowers-btn"),
  unlockDiscoveriesBtn: document.getElementById("unlock-discoveries-btn"),
  unlockLevelsBtn: document.getElementById("unlock-levels-btn"),
  unlockGaragesBtn: document.getElementById("unlock-garages-btn"),
  unlockUpgradesBtn: document.getElementById("unlock-upgrades-btn"),
  garageUpgradeAll: document.getElementById("garage-upgrade-all"),
  rulesEditor: document.getElementById("rules-editor"),
  rulesRandom: document.getElementById("rules-random"),
  applyRulesBtn: document.getElementById("apply-rules-btn"),
  trialsList: document.getElementById("trials-list"),
  trialsSelectAllBtn: document.getElementById("trials-select-all-btn"),
  trialsClearBtn: document.getElementById("trials-clear-btn"),
  saveTrialsBtn: document.getElementById("save-trials-btn"),
  prosRegistration: document.getElementById("pros-registration"),
  prosRoadcraft: document.getElementById("pros-roadcraft"),
  saveProsBtn: document.getElementById("save-pros-btn"),
  achievementsStats: document.getElementById("achievements-stats"),
  unlockAchievementsBtn: document.getElementById("unlock-achievements-btn"),
  fogFileSelect: document.getElementById("fog-file-select"),
  fogBrushSize: document.getElementById("fog-brush-size"),
  fogBrushColor: document.getElementById("fog-brush-color"),
  fogOpenBtn: document.getElementById("fog-open-btn"),
  fogSaveBtn: document.getElementById("fog-save-btn"),
  fogDownloadCurrentBtn: document.getElementById("fog-download-current-btn"),
  fogEditorInfo: document.getElementById("fog-editor-info"),
  fogCanvas: document.getElementById("fog-canvas"),
  fogSlotSelect: document.getElementById("fog-slot-select"),
  fogFilterMode: document.getElementById("fog-filter-mode"),
  fogExtraSeasons: document.getElementById("fog-extra-seasons"),
  fogRegionFilters: document.getElementById("fog-region-filters"),
  fogCoverBtn: document.getElementById("fog-cover-btn"),
  fogUncoverBtn: document.getElementById("fog-uncover-btn"),
  fogAutoInfo: document.getElementById("fog-auto-info"),
};

function init() {
  state.selectors.missions = buildSeasonMapSelector(document.getElementById("missions-selector"), "missions");
  state.selectors.contests = buildSeasonMapSelector(document.getElementById("contests-selector"), "contests");
  state.selectors.regions = buildRegionSelector(document.getElementById("regions-selector"), "regions");
  state.selectors.upgrades = buildRegionSelector(document.getElementById("upgrades-selector"), "upgrades");
  state.selectors.watchtowers = buildRegionSelector(document.getElementById("watchtowers-selector"), "watchtowers");
  state.selectors.discoveries = buildRegionSelector(document.getElementById("discoveries-selector"), "discoveries");
  state.selectors.levels = buildRegionSelector(document.getElementById("levels-selector"), "levels");
  state.selectors.garages = buildRegionSelector(document.getElementById("garages-selector"), "garages");
  buildRegionsFeatureEditor();
  onRegionsLegacyToggleChanged();
  buildRulesEditor();
  renderRankXpTable();
  scheduleObjectivesCatalogWarmup();
  renderTrialsList();
  initFogTools();
  bindUi();
  updateMainMeta();
  updateCommonMeta();
  updateImproveShareMeta();
  updateFolderMeta();
  refreshFolderMainChoices();
  updateMainSummary();
  refreshCommonTabs();
  refreshFogFileList();
  renderFogCanvas();
  checkRuntimeDependencies();
  scheduleAnalyticsInit();
  updateDownloadButtons();
}

function bindUi() {
  if (els.folderUploadBtn && els.folderInput) {
    els.folderUploadBtn.addEventListener("click", () => els.folderInput.click());
  }
  if (els.singleUploadBtn && els.singleInput) {
    els.singleUploadBtn.addEventListener("click", () => els.singleInput.click());
  }
  if (els.mainInput) {
    els.mainInput.addEventListener("change", onMainFileSelected);
  }
  if (els.commonInput) {
    els.commonInput.addEventListener("change", onCommonFileSelected);
  }
  if (els.folderInput) {
    els.folderInput.addEventListener("change", onFolderSelected);
  }
  if (els.improveShareCheckbox) {
    els.improveShareCheckbox.addEventListener("change", onImproveShareCheckboxChanged);
  }
  if (els.singleInput) {
    els.singleInput.addEventListener("change", onSingleFileSelected);
  }
  els.downloadMainBtn.addEventListener("click", downloadMainFile);
  els.downloadCommonBtn.addEventListener("click", downloadCommonFile);
  els.downloadFolderBtn.addEventListener("click", downloadFolderZip);

  els.tabNav.addEventListener("click", (event) => {
    const button = event.target.closest(".tab-btn");
    if (!button) {
      return;
    }
    activateTab(button.dataset.tab);
  });

  els.refreshGameStatsBtn.addEventListener("click", refreshGameStatsEditor);
  els.saveGameStatsBtn.addEventListener("click", saveGameStatsToMain);
  els.rankInput.addEventListener("input", syncXpFromRankInput);
  els.xpInput.addEventListener("input", syncRankFromXpInput);
  els.applyMoneyRankBtn.addEventListener("click", onApplyMoneyRank);
  els.applyTimeBtn.addEventListener("click", onApplyTime);
  els.applyMissionsBtn.addEventListener("click", onApplyMissions);
  els.applyContestsBtn.addEventListener("click", onApplyContests);
  els.unlockWatchBtn.addEventListener("click", () => onApplyRegionWithSelector(state.selectors.watchtowers, unlockWatchtowers, "Watchtowers updated."));
  els.unlockDiscoveriesBtn.addEventListener("click", () => onApplyRegionWithSelector(state.selectors.discoveries, unlockDiscoveries, "Discoveries updated."));
  els.unlockLevelsBtn.addEventListener("click", () => onApplyRegionWithSelector(state.selectors.levels, unlockLevels, "Levels updated."));
  els.unlockGaragesBtn.addEventListener("click", () => onApplyRegionWithSelector(state.selectors.garages, (text, regions) => unlockGarages(text, regions, Boolean(els.garageUpgradeAll.checked)), "Garages updated."));
  els.unlockUpgradesBtn.addEventListener("click", () => onApplyRegionWithSelector(state.selectors.upgrades, unlockUpgrades, "Upgrades updated."));
  els.refreshObjectivesBtn.addEventListener("click", refreshObjectivesFromMain);
  els.selectObjectivesBtn.addEventListener("click", selectVisibleObjectives);
  els.clearObjectivesBtn.addEventListener("click", clearObjectiveSelection);
  els.completeObjectivesBtn.addEventListener("click", completeSelectedObjectives);
  els.objectivesSearch.addEventListener("input", renderObjectivesList);
  els.objectivesType.addEventListener("change", renderObjectivesList);
  els.objectivesRegion.addEventListener("change", renderObjectivesList);
  els.objectivesCategory.addEventListener("change", renderObjectivesList);
  if (els.regionsCheckAllBtn) {
    els.regionsCheckAllBtn.addEventListener("click", () => setAllRegionsFeatures(true));
  }
  if (els.regionsClearBtn) {
    els.regionsClearBtn.addEventListener("click", () => setAllRegionsFeatures(false));
  }
  if (els.regionsShowLegacy) {
    els.regionsShowLegacy.addEventListener("change", onRegionsLegacyToggleChanged);
  }
  if (els.applyRegionsBtn) {
    els.applyRegionsBtn.addEventListener("click", onApplyRegions);
  }
  if (els.rulesRandom) {
    els.rulesRandom.addEventListener("change", onRulesRandomToggleChanged);
  }
  els.applyRulesBtn.addEventListener("click", onApplyRules);

  els.trialsSelectAllBtn.addEventListener("click", () => setAllTrials(true));
  els.trialsClearBtn.addEventListener("click", () => setAllTrials(false));
  els.saveTrialsBtn.addEventListener("click", onSaveTrials);
  els.saveProsBtn.addEventListener("click", onSavePros);
  els.unlockAchievementsBtn.addEventListener("click", onUnlockAchievements);

  if (els.fogBrushColor) {
    els.fogBrushColor.addEventListener("change", onFogBrushChanged);
  }
  if (els.fogBrushSize) {
    els.fogBrushSize.addEventListener("change", onFogBrushChanged);
  }
  bindAsyncUiAction(els.fogOpenBtn, "Open fog file", onOpenSelectedFogFile);
  bindAsyncUiAction(els.fogSaveBtn, "Save fog file", onSaveCurrentFogToFolder);
  bindAsyncUiAction(els.fogDownloadCurrentBtn, "Download fog file", downloadCurrentFogFile);
  bindAsyncUiAction(els.fogFileSelect, "Open fog file", onOpenSelectedFogFile, "change");
  bindAsyncUiAction(els.fogCoverBtn, "Fog automation cover", () => runFogAutomation("cover"));
  bindAsyncUiAction(els.fogUncoverBtn, "Fog automation uncover", () => runFogAutomation("uncover"));

  const fogCanvas = els.fogCanvas;
  if (fogCanvas) {
    fogCanvas.addEventListener("pointerdown", onFogPointerDown);
    fogCanvas.addEventListener("pointermove", onFogPointerMove);
    fogCanvas.addEventListener("pointerup", onFogPointerUp);
    fogCanvas.addEventListener("pointerleave", onFogPointerUp);
    fogCanvas.addEventListener("pointercancel", onFogPointerUp);
  }
  window.addEventListener("resize", renderFogCanvas);
}

function bindAsyncUiAction(element, label, handler, eventName = "click") {
  if (!element || typeof element.addEventListener !== "function") {
    return;
  }
  element.addEventListener(eventName, (event) => {
    Promise.resolve()
      .then(() => handler(event))
      .catch((error) => {
        const message = error && error.message ? error.message : String(error || "Unknown error");
        setStatus(`${label} failed: ${message}`, "error");
        if (els.fogAutoInfo && /fog/i.test(label)) {
          els.fogAutoInfo.textContent = `${label} failed: ${message}`;
        }
      });
  });
}

function checkRuntimeDependencies() {
  // Heavy libraries are loaded on demand (fog tools / folder zip).
}

function isVendorReady(vendor) {
  if (vendor === "pako") {
    return Boolean(window.pako && typeof window.pako.Inflate === "function");
  }
  if (vendor === "jszip") {
    return Boolean(window.JSZip);
  }
  return false;
}

function loadVendorScript(vendor) {
  const src = VENDOR_SCRIPT_URLS[vendor];
  if (!src) {
    return Promise.reject(new Error(`Unknown vendor: ${vendor}`));
  }
  if (isVendorReady(vendor)) {
    return Promise.resolve();
  }
  if (vendorLoadPromises[vendor]) {
    return vendorLoadPromises[vendor];
  }
  vendorLoadPromises[vendor] = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = () => {
      if (isVendorReady(vendor)) {
        resolve();
      } else {
        reject(new Error(`${vendor} loaded but is not available.`));
      }
    };
    script.onerror = () => reject(new Error(`Failed to load ${vendor} runtime.`));
    document.head.append(script);
  }).finally(() => {
    vendorLoadPromises[vendor] = null;
  });
  return vendorLoadPromises[vendor];
}

async function ensurePakoLoaded() {
  if (isVendorReady("pako")) {
    return true;
  }
  try {
    await loadVendorScript("pako");
    return isVendorReady("pako");
  } catch (_error) {
    return false;
  }
}

async function ensureJsZipLoaded() {
  if (isVendorReady("jszip")) {
    return true;
  }
  try {
    await loadVendorScript("jszip");
    return isVendorReady("jszip");
  } catch (_error) {
    return false;
  }
}

function scheduleAnalyticsInit() {
  if (!GA_MEASUREMENT_ID || typeof window === "undefined") {
    return;
  }
  const init = () => {
    if (window.__analyticsInitialized) {
      return;
    }
    window.__analyticsInitialized = true;
    window.dataLayer = window.dataLayer || [];
    window.gtag = window.gtag || function gtag() {
      window.dataLayer.push(arguments);
    };
    window.gtag("js", new Date());
    window.gtag("config", GA_MEASUREMENT_ID);
    const script = document.createElement("script");
    script.async = true;
    script.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(GA_MEASUREMENT_ID)}`;
    document.head.append(script);
  };
  if (typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(init, { timeout: 4000 });
  } else {
    window.setTimeout(init, 2500);
  }
}

async function onMainFileSelected() {
  const file = els.mainInput.files && els.mainInput.files[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    setMainFromText(file.name, text, null);
    setStatus(buildMainLoadStatus(`Loaded main save: ${file.name}`), "success");
  } catch (error) {
    setStatus(`Failed to read main save: ${error.message}`, "error");
  } finally {
    els.mainInput.value = "";
  }
}

async function onCommonFileSelected() {
  const file = els.commonInput.files && els.commonInput.files[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    setCommonFromText(file.name, text, null);
    setStatus(`Loaded CommonSslSave: ${file.name}`, "success");
  } catch (error) {
    setStatus(`Failed to read CommonSslSave: ${error.message}`, "error");
  } finally {
    els.commonInput.value = "";
  }
}

async function onSingleFileSelected() {
  const file = els.singleInput.files && els.singleInput.files[0];
  if (!file) {
    return;
  }
  try {
    const bytes = new Uint8Array(await file.arrayBuffer());
    const text = decodeBytesToText(bytes);
    const role = detectSingleFileRole(file.name, text);
    if (role === "common") {
      setCommonFromText(file.name, text, null);
      setStatus(`Loaded single file as CommonSslSave: ${file.name}`, "success");
    } else {
      setMainFromText(file.name, text, null);
      setStatus(buildMainLoadStatus(`Loaded single file as Main Save: ${file.name}`), "success");
    }
    state.improveShare.preferredSource = "single";
    state.improveShare.lastUploadedSignature = "";
    if (els.improveShareCheckbox && els.improveShareCheckbox.checked) {
      const standaloneEntries = getStandaloneLoadedEntriesForImproveShare();
      await maybeUploadImproveSamples(standaloneEntries.length > 0 ? standaloneEntries : [{
        key: String(file.name || "single-file").toLowerCase(),
        relPath: String(file.name || "single-file"),
        name: String(file.name || "single-file"),
        bytes,
        dirty: false,
      }]);
    }
  } catch (error) {
    setStatus(`Failed to read single file: ${error.message}`, "error");
  } finally {
    els.singleInput.value = "";
  }
}

function normalizeRelativePath(path) {
  return String(path || "").replace(/\\/g, "/").replace(/^\/+|\/+$/g, "");
}

function joinRelativePath(...parts) {
  return parts
    .map((part) => normalizeRelativePath(part))
    .filter(Boolean)
    .join("/");
}

function stripFolderRoot(relativePath, rootName) {
  const normalized = normalizeRelativePath(relativePath);
  const root = normalizeRelativePath(rootName);
  if (!root) {
    return normalized;
  }
  const lower = normalized.toLowerCase();
  const rootLower = root.toLowerCase();
  if (lower === rootLower) {
    return "";
  }
  if (lower.startsWith(`${rootLower}/`)) {
    return normalized.slice(root.length + 1);
  }
  return normalized;
}

function isSameOrChildRelative(path, parent) {
  const normalizedPath = normalizeRelativePath(path).toLowerCase();
  const normalizedParent = normalizeRelativePath(parent).toLowerCase();
  if (!normalizedParent) {
    return true;
  }
  return normalizedPath === normalizedParent || normalizedPath.startsWith(`${normalizedParent}/`);
}

function buildUploadedFolderEntries(files) {
  const relativePaths = (files || []).map((file) => String(file.webkitRelativePath || file.name || ""));
  const rootName = detectFolderRoot(relativePaths);
  return Promise.all((files || []).map(async (file) => {
    const relPath = normalizeRelativePath(file.webkitRelativePath || file.name || "");
    const bytes = new Uint8Array(await file.arrayBuffer());
    const relPathNoRoot = stripFolderRoot(relPath, rootName);
    return {
      key: String(relPathNoRoot || relPath || file.name).toLowerCase(),
      relPath,
      relPathNoRoot,
      name: getFileBasename(relPath || file.name),
      bytes,
      dirty: false,
    };
  })).then((entries) => ({ rootName, entries }));
}

function containsWgsMarkers(entries) {
  return (entries || []).some((entry) => {
    const lower = String(entry && entry.name ? entry.name : "").toLowerCase();
    return lower === "containers.index" || lower.startsWith("container.");
  });
}

function buildRawEntryLookup(entries) {
  const out = new Map();
  for (const entry of entries || []) {
    const key = String(entry && (entry.relPathNoRoot || entry.relPath || entry.name) ? (entry.relPathNoRoot || entry.relPath || entry.name) : "").toLowerCase();
    if (!key || out.has(key)) {
      continue;
    }
    out.set(key, entry);
  }
  return out;
}

function findRawEntry(entriesMap, relativePath) {
  if (!(entriesMap instanceof Map)) {
    return null;
  }
  return entriesMap.get(String(normalizeRelativePath(relativePath)).toLowerCase()) || null;
}

function discoverWgsCandidates(entries) {
  const userDirs = new Set();
  const containerDirs = new Set();
  for (const entry of entries || []) {
    const rel = normalizeRelativePath(entry && entry.relPathNoRoot ? entry.relPathNoRoot : "");
    const base = getFileBasename(rel).toLowerCase();
    const dir = normalizeRelativePath(rel.slice(0, Math.max(0, rel.length - base.length)).replace(/\/$/, ""));
    if (base === "containers.index") {
      userDirs.add(dir);
    } else if (base.startsWith("container.")) {
      containerDirs.add(dir);
    }
  }
  const userList = [...userDirs];
  if (userList.length > 0) {
    return userList.map((path) => ({ kind: "user_dir", path }));
  }
  const filteredContainers = [...containerDirs].filter((dir) => !userList.some((userDir) => isSameOrChildRelative(dir, userDir)));
  return filteredContainers.map((path) => ({ kind: "container_dir", path }));
}

function readUint32LE(view, offset) {
  return view.getUint32(offset, true);
}

function readInt32LE(view, offset) {
  return view.getInt32(offset, true);
}

function bytesToHex(bytes) {
  return Array.from(bytes || [], (value) => value.toString(16).padStart(2, "0")).join("").toUpperCase();
}

function readUtf16LeString(view, cursor, fixedChars = null) {
  let charCount = fixedChars;
  if (charCount == null) {
    if (cursor.offset + 4 > view.byteLength) {
      throw new Error("Unexpected end of WGS data while reading string length.");
    }
    charCount = readInt32LE(view, cursor.offset);
    cursor.offset += 4;
  }
  const byteLength = Math.max(0, Number(charCount) || 0) * 2;
  if (cursor.offset + byteLength > view.byteLength) {
    throw new Error("Unexpected end of WGS data while reading UTF-16 string.");
  }
  const bytes = new Uint8Array(view.buffer, view.byteOffset + cursor.offset, byteLength);
  cursor.offset += byteLength;
  return new TextDecoder("utf-16le").decode(bytes).replace(/\0+$/g, "");
}

function readFiletime(view, cursor) {
  if (cursor.offset + 8 > view.byteLength) {
    throw new Error("Unexpected end of WGS data while reading FILETIME.");
  }
  cursor.offset += 8;
}

function readGuidHexLe(view, cursor) {
  if (cursor.offset + 16 > view.byteLength) {
    throw new Error("Unexpected end of WGS data while reading GUID.");
  }
  const raw = new Uint8Array(view.buffer, view.byteOffset + cursor.offset, 16);
  cursor.offset += 16;
  const reordered = new Uint8Array(16);
  reordered.set([raw[3], raw[2], raw[1], raw[0]], 0);
  reordered.set([raw[5], raw[4]], 4);
  reordered.set([raw[7], raw[6]], 6);
  reordered.set(raw.subarray(8, 16), 8);
  return bytesToHex(reordered);
}

function pickWgsBlobRelativePath(entriesMap, containerDir, guidA, guidB) {
  const pathA = joinRelativePath(containerDir, guidA);
  const pathB = joinRelativePath(containerDir, guidB);
  const entryA = findRawEntry(entriesMap, pathA);
  const entryB = findRawEntry(entriesMap, pathB);
  if (guidA === guidB) {
    return entryA ? pathA : null;
  }
  if (entryA && !entryB) {
    return pathA;
  }
  if (entryB && !entryA) {
    return pathB;
  }
  if (entryB) {
    return pathB;
  }
  if (entryA) {
    return pathA;
  }
  return null;
}

function readWgsContainerEntries(containerEntry, containerDir, entriesMap) {
  const view = new DataView(containerEntry.bytes.buffer, containerEntry.bytes.byteOffset, containerEntry.bytes.byteLength);
  const cursor = { offset: 0 };
  if (view.byteLength < 8) {
    throw new Error(`Container file is incomplete: ${containerEntry.relPath}`);
  }
  cursor.offset += 4;
  const fileCount = readInt32LE(view, cursor.offset);
  cursor.offset += 4;
  const out = [];
  for (let i = 0; i < fileCount; i += 1) {
    const logicalName = readUtf16LeString(view, cursor, 64);
    const guidA = readGuidHexLe(view, cursor);
    const guidB = readGuidHexLe(view, cursor);
    const blobRelPath = pickWgsBlobRelativePath(entriesMap, containerDir, guidA, guidB);
    out.push({
      logicalName,
      blobRelPath,
      containerRelPath: containerEntry.relPathNoRoot || containerEntry.relPath,
    });
  }
  return out;
}

function readWgsUserDirEntries(candidatePath, entriesMap) {
  const indexEntry = findRawEntry(entriesMap, joinRelativePath(candidatePath, "containers.index"));
  if (!indexEntry) {
    throw new Error("The selected WGS user folder is missing containers.index.");
  }
  const view = new DataView(indexEntry.bytes.buffer, indexEntry.bytes.byteOffset, indexEntry.bytes.byteLength);
  const cursor = { offset: 0 };
  if (view.byteLength < 8) {
    throw new Error("The WGS containers.index file is incomplete.");
  }
  cursor.offset += 4;
  const containerCount = readInt32LE(view, cursor.offset);
  cursor.offset += 4;
  readUtf16LeString(view, cursor);
  const packageName = readUtf16LeString(view, cursor).split("!")[0];
  readFiletime(view, cursor);
  if (cursor.offset + 4 > view.byteLength) {
    throw new Error("The WGS containers.index file is incomplete.");
  }
  cursor.offset += 4;
  readUtf16LeString(view, cursor);
  if (cursor.offset + 8 > view.byteLength) {
    throw new Error("The WGS containers.index file is incomplete.");
  }
  cursor.offset += 8;

  const out = [];
  for (let i = 0; i < containerCount; i += 1) {
    readUtf16LeString(view, cursor);
    readUtf16LeString(view, cursor);
    readUtf16LeString(view, cursor);
    if (cursor.offset + 1 > view.byteLength) {
      throw new Error("The WGS containers.index file is incomplete.");
    }
    const containerNum = view.getUint8(cursor.offset);
    cursor.offset += 1;
    if (cursor.offset + 4 > view.byteLength) {
      throw new Error("The WGS containers.index file is incomplete.");
    }
    cursor.offset += 4;
    const containerGuid = readGuidHexLe(view, cursor);
    readFiletime(view, cursor);
    if (cursor.offset + 16 > view.byteLength) {
      throw new Error("The WGS containers.index file is incomplete.");
    }
    cursor.offset += 16;

    const containerDir = joinRelativePath(candidatePath, containerGuid);
    const containerRelPath = joinRelativePath(containerDir, `container.${containerNum}`);
    const containerEntry = findRawEntry(entriesMap, containerRelPath);
    if (!containerEntry) {
      continue;
    }
    out.push(...readWgsContainerEntries(containerEntry, containerDir, entriesMap));
  }

  if (packageName && !packageName.toLowerCase().includes("snowrunner")) {
    throw new Error(`The selected WGS user folder belongs to a different package: ${packageName}`);
  }
  return out;
}

function readWgsContainerDirEntries(candidatePath, entriesMap) {
  const out = [];
  for (const entry of entriesMap.values()) {
    const rel = normalizeRelativePath(entry && entry.relPathNoRoot ? entry.relPathNoRoot : "");
    const base = getFileBasename(rel).toLowerCase();
    const dir = normalizeRelativePath(rel.slice(0, Math.max(0, rel.length - base.length)).replace(/\/$/, ""));
    if (dir !== normalizeRelativePath(candidatePath) || !base.startsWith("container.")) {
      continue;
    }
    out.push(...readWgsContainerEntries(entry, candidatePath, entriesMap));
  }
  if (out.length === 0) {
    throw new Error("The selected folder does not contain any WGS container files.");
  }
  return out;
}

function wgsLogicalNameToRelativePath(logicalName) {
  const text = String(logicalName || "").replace(/\0/g, "").trim();
  if (!text) {
    return "";
  }
  const parts = [];
  for (const chunk of text.split(/[\\/]+/)) {
    const trimmed = chunk.trim();
    if (!trimmed || trimmed === "." || trimmed === "..") {
      continue;
    }
    parts.push(trimmed.replace(/[<>:"|?*]/g, "_"));
  }
  if (parts.length === 0) {
    return "";
  }
  let rel = parts.join("/");
  if (!/\.[A-Za-z0-9]+$/.test(rel)) {
    rel += ".cfg";
  }
  return rel;
}

function decodeWgsFolder(entries, rootName) {
  const candidates = discoverWgsCandidates(entries);
  if (candidates.length === 0) {
    return null;
  }
  if (candidates.length > 1) {
    const summary = candidates.map((item) => item.path || "<selected folder>").join(", ");
    throw new Error(`Multiple WGS save sources were found in the uploaded folder. Upload a single WGS user folder or container folder. Found: ${summary}`);
  }

  const candidate = candidates[0];
  const entriesMap = buildRawEntryLookup(entries);
  const logicalEntries = candidate.kind === "user_dir"
    ? readWgsUserDirEntries(candidate.path, entriesMap)
    : readWgsContainerDirEntries(candidate.path, entriesMap);

  const virtualEntries = [];
  const wgsBlobByLogicalKey = new Map();
  const seenLogical = new Set();
  for (const item of logicalEntries) {
    const relPath = wgsLogicalNameToRelativePath(item.logicalName);
    const blobRelPath = normalizeRelativePath(item.blobRelPath);
    if (!relPath || !blobRelPath) {
      continue;
    }
    const blobEntry = findRawEntry(entriesMap, blobRelPath);
    if (!blobEntry) {
      continue;
    }
    const key = relPath.toLowerCase();
    if (seenLogical.has(key)) {
      continue;
    }
    seenLogical.add(key);
    virtualEntries.push({
      key,
      relPath,
      relPathNoRoot: relPath,
      name: getFileBasename(relPath),
      bytes: new Uint8Array(blobEntry.bytes),
      dirty: false,
    });
    wgsBlobByLogicalKey.set(key, blobRelPath);
  }

  if (virtualEntries.length === 0) {
    throw new Error("The selected WGS folder was decoded, but no readable SnowRunner save files were found.");
  }

  return {
    rootName,
    sourceKind: "wgs",
    files: virtualEntries,
    rawFiles: entries,
    wgsBlobByLogicalKey,
  };
}

async function onFolderSelected() {
  const files = els.folderInput.files ? [...els.folderInput.files] : [];
  if (files.length === 0) {
    return;
  }
  try {
    const rawPayload = await buildUploadedFolderEntries(files);
    const allEntries = rawPayload.entries;
    const rootName = rawPayload.rootName;
    const hasWgsMarkers = containsWgsMarkers(allEntries);
    const wgsPayload = hasWgsMarkers ? decodeWgsFolder(allEntries, rootName) : null;

    const acceptedFiles = wgsPayload
      ? wgsPayload.files
      : allEntries.filter((entry) => isTopLevelFolderFile(entry.relPath));
    const ignoredCount = wgsPayload ? 0 : Math.max(0, files.length - acceptedFiles.length);

    state.folder.files.clear();
    state.folder.rawFiles.clear();
    state.folder.wgsBlobByLogicalKey.clear();
    for (const entry of acceptedFiles) {
      state.folder.files.set(entry.key, entry);
    }
    if (wgsPayload) {
      for (const entry of wgsPayload.rawFiles) {
        state.folder.rawFiles.set(String(entry.relPathNoRoot || entry.relPath || entry.name).toLowerCase(), entry);
      }
      for (const [logicalKey, blobRelPath] of wgsPayload.wgsBlobByLogicalKey.entries()) {
        state.folder.wgsBlobByLogicalKey.set(logicalKey, blobRelPath);
      }
      state.folder.sourceKind = "wgs";
    } else {
      state.folder.sourceKind = "standard";
    }
    state.folder.loaded = true;
    state.folder.rootName = rootName;
    state.improveShare.preferredSource = "folder";
    state.improveShare.lastUploadedSignature = "";
    updateFolderMeta();
    refreshFolderMainChoices(acceptedFiles);
    refreshFogFileList();

    const mainEntries = getMainFolderEntries(acceptedFiles);
    if (mainEntries.length === 1) {
      const onlyMain = mainEntries[0];
      const text = decodeBytesToText(onlyMain.bytes);
      setMainFromText(onlyMain.name, text, onlyMain.key);
    } else if (mainEntries.length !== 1) {
      state.main = null;
      updateMainMeta();
      updateMainSummary();
      updateDownloadButtons();
    }
    const commonEntry = pickCommonEntryFromFolder(acceptedFiles);
    if (commonEntry) {
      const text = decodeBytesToText(commonEntry.bytes);
      setCommonFromText(commonEntry.name, text, commonEntry.key);
    }
    updateDownloadButtons();
    const fogCount = getFogFolderEntries().length;
    let folderStatusMessage = "";
    let folderStatusType = "success";
    if (wgsPayload) {
      folderStatusMessage = `Loaded WGS folder: decoded ${acceptedFiles.length} logical files from ${allEntries.length} uploaded files (${fogCount} fog files detected). Download Folder will rebuild the WGS layout as a zip.`;
      folderStatusType = "success";
    } else if (mainEntries.length > 1) {
      folderStatusMessage = `Loaded folder: ${acceptedFiles.length} top-level files (${fogCount} fog files). Ignored ${ignoredCount} subfolder file(s). Pick your CompleteSave* file below Upload Save Folder or File.`;
      folderStatusType = "info";
    } else {
      folderStatusMessage = `Loaded folder: ${acceptedFiles.length} top-level files (${fogCount} fog files detected). Ignored ${ignoredCount} subfolder file(s).`;
    }
    const warningSuffix = getMainVersionWarningSuffix();
    if (warningSuffix) {
      folderStatusMessage += warningSuffix;
    }
    setStatus(folderStatusMessage, folderStatusType);
    await maybeUploadImproveSamples(acceptedFiles);
  } catch (error) {
    setStatus(`Failed to load folder: ${error.message}`, "error");
  } finally {
    els.folderInput.value = "";
  }
}

function activateTab(tab) {
  for (const btn of document.querySelectorAll(".tab-btn")) {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  }
  for (const panel of document.querySelectorAll(".tab-panel")) {
    panel.classList.toggle("active", panel.id === `panel-${tab}`);
  }
  if (tab === "save") {
    refreshGameStatsEditor();
  }
  if (tab === "objectives") {
    ensureObjectivesCatalogLoaded(true);
    renderObjectivesList();
  }
  if (tab === "fog") {
    ensurePakoLoaded();
    refreshFogFileList();
    renderFogCanvas();
  }
  if (tab === "trials" || tab === "pros" || tab === "achievements") {
    refreshCommonTabs();
  }
}

function updateMainMeta() {
  if (!state.main) {
    els.mainMeta.textContent = "No file loaded.";
    return;
  }
  const size = (state.main.text.length / 1024).toFixed(1);
  const stateText = state.main.dirty ? "edited" : "original";
  els.mainMeta.textContent = `${state.main.name} • ${size} KB • ${stateText}`;
}

function updateCommonMeta() {
  if (!state.common) {
    els.commonMeta.textContent = "No file loaded.";
    return;
  }
  const size = (state.common.text.length / 1024).toFixed(1);
  const stateText = state.common.dirty ? "edited" : "original";
  els.commonMeta.textContent = `${state.common.name} • ${size} KB • ${stateText}`;
}

function updateFolderMeta() {
  if (!state.folder.loaded || state.folder.files.size === 0) {
    els.folderMeta.textContent = "No folder loaded.";
    return;
  }
  let edited = 0;
  for (const entry of state.folder.files.values()) {
    if (entry.dirty) {
      edited += 1;
    }
  }
  const fogCount = getFogFolderEntries().length;
  const root = state.folder.rootName ? `${state.folder.rootName} • ` : "";
  const sourceLabel = state.folder.sourceKind === "wgs" ? "WGS decoded • " : "";
  els.folderMeta.textContent = `${root}${sourceLabel}${state.folder.files.size} files • ${fogCount} fog files • ${edited} edited`;
}

function refreshFolderMainChoices(entriesOverride) {
  if (!els.folderMainChoices) {
    return;
  }
  const entries = getMainFolderEntries(entriesOverride);
  els.folderMainChoices.innerHTML = "";
  if (entries.length === 0) {
    const text = document.createElement("span");
    text.className = "meta";
    text.textContent = "No CompleteSave*.cfg/.dat files or decoded WGS main saves detected.";
    els.folderMainChoices.append(text);
    return;
  }
  for (const entry of entries) {
    const btn = makeButton(entry.name + (entry.dirty ? " (edited)" : ""), "btn folder-choice-btn");
    if (state.main && state.main.folderKey === entry.key) {
      btn.classList.add("active");
    }
    btn.addEventListener("click", () => loadMainFromFolderKey(entry.key));
    els.folderMainChoices.append(btn);
  }
}

function loadMainFromFolderKey(key) {
  if (!key) {
    return;
  }
  const entry = state.folder.files.get(String(key).toLowerCase());
  if (!entry) {
    setStatus("Selected CompleteSave file not found.", "error");
    return;
  }
  try {
    const text = decodeBytesToText(entry.bytes);
    setMainFromText(entry.name, text, entry.key);
    setStatus(buildMainLoadStatus(`Active main save switched to ${entry.name}`), "success");
  } catch (error) {
    setStatus(`Failed to load selected main save: ${error.message}`, "error");
  }
}

function setMainFromText(name, text, folderKey) {
  const slotIndex = extractCompleteSaveIndex(name);
  const versionDiffs = readSaveVersionDiffs(text);
  state.main = {
    name,
    text,
    dirty: false,
    folderKey: folderKey || null,
    versionDiffs,
  };
  const slotNumber = slotIndex + 1;
  if (els.fogSlotSelect && Number.isInteger(slotNumber) && slotNumber >= 1 && slotNumber <= 4) {
    els.fogSlotSelect.value = String(slotNumber);
  }
  if (!folderKey) {
    state.improveShare.preferredSource = "single";
  }
  updateMainMeta();
  updateMainSummary();
  refreshFolderMainChoices();
  updateDownloadButtons();
}

function buildMainLoadStatus(prefix) {
  return `${prefix}${getMainVersionWarningSuffix()}`;
}

function getMainVersionWarningSuffix() {
  if (!state.main || !Array.isArray(state.main.versionDiffs) || state.main.versionDiffs.length === 0) {
    return "";
  }
  return ` | Version warning: ${state.main.versionDiffs.join("; ")}`;
}

function readSaveVersionDiffs(content) {
  const diffs = [];
  for (const [key, expected] of Object.entries(SAVE_VERSION_EXPECTED)) {
    const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(-?\\d+)`, "i");
    const m = re.exec(content);
    if (!m) {
      diffs.push(`${key}=missing (expected ${expected})`);
      continue;
    }
    const value = Number.parseInt(m[1], 10);
    if (!Number.isFinite(value)) {
      diffs.push(`${key}=unreadable (expected ${expected})`);
      continue;
    }
    if (value !== expected) {
      diffs.push(`${key}=${value} (expected ${expected})`);
    }
  }
  return diffs;
}

function setCommonFromText(name, text, folderKey) {
  state.common = {
    name,
    text,
    dirty: false,
    folderKey: folderKey || null,
  };
  if (!folderKey) {
    state.improveShare.preferredSource = "single";
  }
  updateCommonMeta();
  refreshCommonTabs();
  updateDownloadButtons();
}

function syncMainToFolderEntry() {
  if (!state.main || !state.main.folderKey) {
    return;
  }
  const entry = state.folder.files.get(state.main.folderKey);
  if (!entry) {
    return;
  }
  entry.bytes = encodeTextToBytes(state.main.text);
  entry.dirty = true;
  updateFolderMeta();
  refreshFolderMainChoices();
}

function syncCommonToFolderEntry() {
  if (!state.common || !state.common.folderKey) {
    return;
  }
  const entry = state.folder.files.get(state.common.folderKey);
  if (!entry) {
    return;
  }
  entry.bytes = encodeTextToBytes(state.common.text);
  entry.dirty = true;
  updateFolderMeta();
}

function updateDownloadButtons() {
  els.downloadMainBtn.disabled = !state.main;
  els.downloadCommonBtn.disabled = !state.common;
  els.downloadFolderBtn.disabled = !state.folder.loaded || state.folder.files.size === 0;
}

function updateMainSummary() {
  if (!state.main) {
    els.gameStatsInfo.textContent = "No file loaded.";
    els.distanceList.innerHTML = "";
    els.gameStatList.innerHTML = "";
    state.objectives.data = null;
    state.objectives.selected.clear();
    state.objectives.visibleKeys = [];
    renderObjectivesList();
    hydrateRulesFromMain();
    return;
  }
  try {
    const info = getFileInfo(state.main.text);

    if (info.money != null) {
      els.moneyInput.value = String(info.money);
    }
    if (info.rank != null) {
      els.rankInput.value = String(info.rank);
    }
    if (info.xp != null) {
      els.xpInput.value = String(info.xp);
    }
    if (info.day != null) {
      els.timeDayInput.value = String(info.day);
    }
    if (info.night != null) {
      els.timeNightInput.value = String(info.night);
    }
    if (info.skipTime != null) {
      els.skipTimeInput.checked = Boolean(info.skipTime);
    }
  } catch (_err) {
    // ignore parse errors for these quick fields
  }
  refreshGameStatsEditor();
  refreshObjectivesFromMain();
  hydrateRulesFromMain();
}

function refreshCommonTabs() {
  refreshTrialsFromCommon();
  refreshProsFromCommon();
  refreshAchievementsFromCommon();
}

function setStatus(message, type = "info") {
  els.status.textContent = message;
  els.status.classList.remove("info", "success", "error");
  els.status.classList.add(type);
}

function requireMain() {
  if (state.main) {
    return true;
  }
  setStatus("This action needs a Main Save file. Use Upload Save Folder or File.", "error");
  try {
    els.mainInput.click();
  } catch (_err) {
    // ignore
  }
  return false;
}

function requireCommon() {
  if (state.common) {
    return true;
  }
  setStatus("This action needs a CommonSslSave file. Use Upload Save Folder or File.", "error");
  try {
    els.commonInput.click();
  } catch (_err) {
    // ignore
  }
  return false;
}

function commitMain(newText, message) {
  if (!state.main) {
    return;
  }
  if (newText === state.main.text) {
    setStatus(message || "No changes were applied.", "info");
    return;
  }
  state.main.text = newText;
  state.main.dirty = true;
  state.main.versionDiffs = readSaveVersionDiffs(newText);
  syncMainToFolderEntry();
  updateMainMeta();
  updateMainSummary();
  updateDownloadButtons();
  setStatus(message || "Main save updated.", "success");
}

function commitCommon(newText, message) {
  if (!state.common) {
    return;
  }
  if (newText === state.common.text) {
    setStatus(message || "No changes were applied.", "info");
    return;
  }
  state.common.text = newText;
  state.common.dirty = true;
  syncCommonToFolderEntry();
  updateCommonMeta();
  refreshCommonTabs();
  updateDownloadButtons();
  setStatus(message || "CommonSslSave updated.", "success");
}

function downloadMainFile() {
  if (!requireMain()) {
    return;
  }
  const name = String(state.main.name || "MainSave.cfg");
  downloadText(name, state.main.text);
}

function downloadCommonFile() {
  if (!requireCommon()) {
    return;
  }
  const name = String(state.common.name || "CompleteSave.cfg");
  downloadText(name, state.common.text);
}

async function downloadFolderZip() {
  if (!state.folder.loaded || state.folder.files.size === 0) {
    setStatus("Upload a save folder first.", "error");
    return;
  }
  const jsZipReady = await ensureJsZipLoaded();
  if (!jsZipReady || !window.JSZip) {
    setStatus("Failed to load JSZip runtime.", "error");
    return;
  }
  try {
    const zip = new window.JSZip();
    if (state.folder.sourceKind === "wgs") {
      const blobOverrides = new Map();
      for (const entry of state.folder.files.values()) {
        const blobRelPath = state.folder.wgsBlobByLogicalKey.get(String(entry.key || "").toLowerCase());
        if (blobRelPath) {
          blobOverrides.set(String(blobRelPath).toLowerCase(), entry.bytes);
        }
      }
      for (const rawEntry of state.folder.rawFiles.values()) {
        const relPath = rawEntry.relPath || rawEntry.name;
        const override = blobOverrides.get(String(rawEntry.relPathNoRoot || rawEntry.relPath || rawEntry.name).toLowerCase());
        zip.file(relPath, override || rawEntry.bytes);
      }
    } else {
      for (const entry of state.folder.files.values()) {
        zip.file(entry.relPath || entry.name, entry.bytes);
      }
    }
    const root = state.folder.rootName || (state.folder.sourceKind === "wgs" ? "wgs-save" : "save-folder");
    const filename = `${root}.zip`;
    const blob = await zip.generateAsync({
      type: "blob",
      compression: "DEFLATE",
      compressionOptions: { level: 6 },
    });
    triggerBlobDownload(filename, blob);
  } catch (error) {
    setStatus(`Failed to build folder zip: ${error.message}`, "error");
  }
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  triggerBlobDownload(filename, blob);
}

function downloadBinary(filename, bytes, contentType = "application/octet-stream") {
  const blob = new Blob([bytes], { type: contentType });
  triggerBlobDownload(filename, blob);
}

function triggerBlobDownload(filename, blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
  setStatus(`Downloaded ${filename}`, "success");
}

function buildSeasonMapSelector(container, prefix) {
  container.innerHTML = "";
  const root = document.createElement("div");
  root.className = "selector-root";

  const actions = document.createElement("div");
  actions.className = "selector-actions";
  const checkAll = makeButton("Check all", "btn");
  const clearAll = makeButton("Clear all", "btn");
  actions.append(checkAll, clearAll);
  root.append(actions);

  const wrap = document.createElement("div");
  wrap.className = "selector-wrap";
  root.append(wrap);

  const seasonsSection = document.createElement("div");
  seasonsSection.className = "selector-section";
  seasonsSection.innerHTML = "<h4>Seasons</h4>";
  const seasonsGrid = document.createElement("div");
  seasonsGrid.className = "checklist-grid";
  seasonsSection.append(seasonsGrid);
  wrap.append(seasonsSection);

  const mapsSection = document.createElement("div");
  mapsSection.className = "selector-section";
  mapsSection.innerHTML = "<h4>Base Maps + Other Season</h4>";
  const mapsGrid = document.createElement("div");
  mapsGrid.className = "checklist-grid";
  mapsSection.append(mapsGrid);
  const otherLabel = document.createElement("label");
  otherLabel.textContent = "Other season number";
  const otherInput = document.createElement("input");
  otherInput.type = "text";
  otherInput.placeholder = "e.g. 18";
  otherLabel.append(otherInput);
  mapsSection.append(otherLabel);
  wrap.append(mapsSection);

  const seasonChecks = new Map();
  for (const season of sortedSeasons()) {
    const id = `${prefix}-season-${season}`;
    const label = SEASON_REGION_MAP[season].label;
    const box = makeCheckbox(id, label);
    seasonChecks.set(season, box.input);
    seasonsGrid.append(box.label);
  }

  const mapChecks = new Map();
  for (const map of BASE_MAPS) {
    const id = `${prefix}-map-${map.code.toLowerCase()}`;
    const box = makeCheckbox(id, map.name);
    mapChecks.set(map.code, box.input);
    mapsGrid.append(box.label);
  }

  checkAll.addEventListener("click", () => {
    for (const input of seasonChecks.values()) {
      input.checked = true;
    }
    for (const input of mapChecks.values()) {
      input.checked = true;
    }
  });
  clearAll.addEventListener("click", () => {
    for (const input of seasonChecks.values()) {
      input.checked = false;
    }
    for (const input of mapChecks.values()) {
      input.checked = false;
    }
    otherInput.value = "";
  });

  container.append(root);

  return {
    getSelectedSeasons() {
      const selected = [];
      for (const [season, input] of seasonChecks.entries()) {
        if (input.checked) {
          selected.push(season);
        }
      }
      const other = Number.parseInt(otherInput.value.trim(), 10);
      if (Number.isInteger(other) && other > 0) {
        selected.push(other);
      }
      return uniqueList(selected);
    },
    getSelectedMaps() {
      const selected = [];
      for (const [code, input] of mapChecks.entries()) {
        if (input.checked) {
          selected.push(code);
        }
      }
      return uniqueList(selected);
    },
  };
}

function buildRegionSelector(container, prefix) {
  container.innerHTML = "";
  const root = document.createElement("div");
  root.className = "selector-root";

  const actions = document.createElement("div");
  actions.className = "selector-actions";
  const checkAll = makeButton("Check all", "btn");
  const clearAll = makeButton("Clear all", "btn");
  actions.append(checkAll, clearAll);
  root.append(actions);

  const wrap = document.createElement("div");
  wrap.className = "selector-wrap";
  root.append(wrap);

  const seasonsSection = document.createElement("div");
  seasonsSection.className = "selector-section";
  seasonsSection.innerHTML = "<h4>Seasons</h4>";
  const seasonsGrid = document.createElement("div");
  seasonsGrid.className = "checklist-grid";
  seasonsSection.append(seasonsGrid);
  wrap.append(seasonsSection);

  const mapsSection = document.createElement("div");
  mapsSection.className = "selector-section";
  mapsSection.innerHTML = "<h4>Base Maps + Other Season</h4>";
  const mapsGrid = document.createElement("div");
  mapsGrid.className = "checklist-grid";
  mapsSection.append(mapsGrid);
  wrap.append(mapsSection);

  const mapChecks = new Map();
  for (const map of BASE_MAPS) {
    const id = `${prefix}-region-${map.code.toLowerCase()}`;
    const box = makeCheckbox(id, map.name);
    mapChecks.set(map.code, box.input);
    mapsGrid.append(box.label);
  }
  const seasonChecks = new Map();
  for (const season of sortedSeasons()) {
    const code = SEASON_REGION_MAP[season].code;
    const id = `${prefix}-region-${code.toLowerCase()}`;
    const box = makeCheckbox(id, SEASON_REGION_MAP[season].label);
    seasonChecks.set(season, box.input);
    seasonsGrid.append(box.label);
  }
  const otherLabel = document.createElement("label");
  otherLabel.textContent = "Other season number";
  const otherInput = document.createElement("input");
  otherInput.type = "text";
  otherInput.placeholder = "e.g. 18";
  otherLabel.append(otherInput);
  mapsSection.append(otherLabel);
  const note = document.createElement("p");
  note.className = "help";
  note.textContent = "Predefined seasons use names only. For new season numbers, type one above.";
  mapsSection.append(note);

  checkAll.addEventListener("click", () => {
    for (const input of mapChecks.values()) {
      input.checked = true;
    }
    for (const input of seasonChecks.values()) {
      input.checked = true;
    }
  });
  clearAll.addEventListener("click", () => {
    for (const input of mapChecks.values()) {
      input.checked = false;
    }
    for (const input of seasonChecks.values()) {
      input.checked = false;
    }
    otherInput.value = "";
  });

  container.append(root);

  return {
    getSelectedSeasons() {
      const selected = [];
      for (const [season, input] of seasonChecks.entries()) {
        if (input.checked) {
          selected.push(season);
        }
      }
      const other = Number.parseInt(otherInput.value.trim(), 10);
      if (Number.isInteger(other) && other > 0) {
        selected.push(other);
      }
      return uniqueList(selected);
    },
    getSelectedMaps() {
      const selected = [];
      for (const [code, input] of mapChecks.entries()) {
        if (input.checked) {
          selected.push(code);
        }
      }
      return uniqueList(selected);
    },
    getSelectedRegions() {
      const selected = [];
      for (const [code, input] of mapChecks.entries()) {
        if (input.checked) {
          selected.push(code);
        }
      }
      for (const [season, input] of seasonChecks.entries()) {
        if (input.checked && SEASON_REGION_MAP[season]) {
          selected.push(SEASON_REGION_MAP[season].code);
        }
      }
      const other = Number.parseInt(otherInput.value.trim(), 10);
      if (Number.isInteger(other) && other > 0) {
        selected.push(`US_${String(other).padStart(2, "0")}`);
      }
      return uniqueList(selected.map((v) => String(v).toUpperCase()));
    },
  };
}

const REGION_FEATURE_DEFINITIONS = [
  {
    key: "missions",
    label: "Missions (Legacy)",
    mode: "seasonMap",
    legacy: true,
    info: "You must accept the task or mission in the game before it can be completed.",
  },
  {
    key: "contests",
    label: "Contests (Legacy)",
    mode: "seasonMap",
    legacy: true,
    info: "You must accept or discover the contests for them to be marked as completed. This also completes unfinished tasks found on the same map.",
  },
  {
    key: "upgrades",
    label: "Upgrades",
    mode: "region",
    info: "At least one upgrade must already be marked or collected in-game for this to work. If a new season is added, you may need to collect or mark one new upgrade first.",
  },
  {
    key: "watchtowers",
    label: "Watchtowers",
    mode: "region",
    info: "This marks watchtowers as found, but it will not reveal the map. Use Fog Tool for that.",
  },
  {
    key: "discoveries",
    label: "Discoveries",
    mode: "region",
    info: "Sets discovered trucks to their max for selected regions but won't add them to the garage.",
  },
  {
    key: "levels",
    label: "Levels",
    mode: "region",
    info: "Lets you view regions you haven't visited yet.",
  },
  {
    key: "garages",
    label: "Garages",
    mode: "region",
    info: "Garages will be unlocked but may still be hidden under fog of war. Recover can stay semi-broken until you find the garage entrance, and some garage entrances are quest-gated.",
    extraKey: "upgradeAll",
    extraLabel: "Upgrade all garages",
  },
];

function buildRegionsFeatureEditor() {
  if (!els.regionsFeatureList) {
    return;
  }
  if (els.regionsShowLegacy) {
    els.regionsShowLegacy.checked = Boolean(state.ui.showLegacyRegionActions);
  }
  els.regionsFeatureList.innerHTML = "";
  state.regions.featureControls.clear();

  for (const def of REGION_FEATURE_DEFINITIONS) {
    const row = document.createElement("div");
    row.className = "regions-feature-row";
    row.dataset.featureKey = def.key;
    if (def.legacy) {
      row.dataset.legacy = "true";
      row.hidden = !state.ui.showLegacyRegionActions;
    }

    const mainLabel = document.createElement("label");
    mainLabel.className = "regions-feature-toggle";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.id = `regions-feature-${def.key}`;
    const text = document.createElement("span");
    text.textContent = def.label;
    mainLabel.append(input, text);

    const infoBtn = document.createElement("button");
    infoBtn.type = "button";
    infoBtn.className = "regions-info-btn";
    infoBtn.textContent = "i";
    infoBtn.title = def.info;
    infoBtn.setAttribute("aria-label", `${def.label} info`);

    row.append(mainLabel, infoBtn);

    let extraInput = null;
    if (def.extraKey) {
      const extraLabel = document.createElement("label");
      extraLabel.className = "regions-feature-extra";
      extraInput = document.createElement("input");
      extraInput.type = "checkbox";
      extraInput.id = `regions-feature-${def.key}-${def.extraKey}`;
      const extraText = document.createElement("span");
      extraText.textContent = def.extraLabel;
      extraLabel.append(extraInput, extraText);
      row.append(extraLabel);
    }

    state.regions.featureControls.set(def.key, { input, extraInput, row, definition: def });
    els.regionsFeatureList.append(row);
  }
}

function onRegionsLegacyToggleChanged() {
  state.ui.showLegacyRegionActions = Boolean(els.regionsShowLegacy && els.regionsShowLegacy.checked);
  for (const control of state.regions.featureControls.values()) {
    if (!control || !control.definition || !control.row) {
      continue;
    }
    if (!control.definition.legacy) {
      continue;
    }
    control.row.hidden = !state.ui.showLegacyRegionActions;
    if (!state.ui.showLegacyRegionActions && control.input) {
      control.input.checked = false;
    }
  }
  if (els.regionsInfo && !state.ui.showLegacyRegionActions) {
    els.regionsInfo.textContent = "Legacy actions hidden. Enable the toggle above if you need Missions / Contests.";
  }
}

function setAllRegionsFeatures(enabled) {
  for (const control of state.regions.featureControls.values()) {
    if (control && control.input) {
      if (control.definition && control.definition.legacy && !state.ui.showLegacyRegionActions) {
        continue;
      }
      control.input.checked = Boolean(enabled);
    }
  }
  if (els.regionsInfo) {
    els.regionsInfo.textContent = enabled ? "All region features selected." : "No region features selected.";
  }
}

function getSelectedRegionsFeatures() {
  const selected = [];
  for (const def of REGION_FEATURE_DEFINITIONS) {
    const control = state.regions.featureControls.get(def.key);
    if (!control || !control.input) {
      continue;
    }
    if (def.legacy && !state.ui.showLegacyRegionActions) {
      continue;
    }
    if (control.input.checked) {
      selected.push({
        definition: def,
        extra: control.extraInput ? Boolean(control.extraInput.checked) : false,
      });
    }
  }
  return selected;
}

function sortedSeasons() {
  return Object.keys(SEASON_REGION_MAP)
    .map(Number)
    .sort((a, b) => a - b);
}

function makeButton(label, className) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = className;
  button.textContent = label;
  return button;
}

function makeCheckbox(id, labelText) {
  const label = document.createElement("label");
  const input = document.createElement("input");
  input.type = "checkbox";
  input.id = id;
  label.setAttribute("for", id);
  label.append(input, document.createTextNode(labelText));
  return { label, input };
}

function initFogTools() {
  if (!els.fogRegionFilters) {
    return;
  }
  els.fogRegionFilters.innerHTML = "";
  state.fog.seasonChecks.clear();
  for (const map of BASE_MAPS) {
    const code = String(map.code || "").toUpperCase();
    const box = makeCheckbox(`fog-region-${code.toLowerCase()}`, map.name);
    state.fog.seasonChecks.set(code, box.input);
    els.fogRegionFilters.append(box.label);
  }
  for (const season of sortedSeasons()) {
    const item = SEASON_REGION_MAP[season];
    if (!item) {
      continue;
    }
    const code = String(item.code || "").toUpperCase();
    const box = makeCheckbox(`fog-region-${code.toLowerCase()}`, item.label || code);
    state.fog.seasonChecks.set(code, box.input);
    els.fogRegionFilters.append(box.label);
  }
  onFogBrushChanged();
}

function onFogBrushChanged() {
  state.fog.brushValue = Math.max(1, Math.min(255, Number.parseInt(els.fogBrushColor.value, 10) || 255));
  state.fog.brushSize = Math.max(1, Math.min(256, Number.parseInt(els.fogBrushSize.value, 10) || 8));
}

function refreshFogFileList() {
  if (!els.fogFileSelect) {
    return;
  }
  const files = getFogFolderEntries();
  const prev = state.fog.currentKey || els.fogFileSelect.value;
  els.fogFileSelect.innerHTML = "";
  if (files.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No fog files loaded";
    els.fogFileSelect.append(opt);
    els.fogFileSelect.disabled = true;
    els.fogOpenBtn.disabled = true;
    els.fogSaveBtn.disabled = true;
    els.fogDownloadCurrentBtn.disabled = true;
    state.fog.currentKey = "";
    state.fog.currentName = "";
    state.fog.currentBytes = null;
    state.fog.currentPixels = null;
    state.fog.currentWidth = 0;
    state.fog.currentHeight = 0;
    state.fog.currentZlibHeader = new Uint8Array([0x78, 0x9c]);
    els.fogEditorInfo.textContent = "Upload a save folder to load fog files.";
    renderFogCanvas();
    return;
  }
  els.fogFileSelect.disabled = false;
  els.fogOpenBtn.disabled = false;
  const currentStillLoaded = Boolean(state.fog.currentPixels && state.fog.currentKey && files.some((entry) => entry.key === state.fog.currentKey));
  els.fogSaveBtn.disabled = !currentStillLoaded;
  els.fogDownloadCurrentBtn.disabled = !currentStillLoaded;
  for (const entry of files) {
    const opt = document.createElement("option");
    opt.value = entry.key;
    const mark = entry.dirty ? " (edited)" : "";
    opt.textContent = `${entry.relPath}${mark}`;
    els.fogFileSelect.append(opt);
  }
  if (prev && files.some((entry) => entry.key === prev)) {
    els.fogFileSelect.value = prev;
  } else {
    els.fogFileSelect.selectedIndex = 0;
  }
  renderFogCanvas();
}

function getFogFolderEntries() {
  if (!state.folder.loaded || state.folder.files.size === 0) {
    return [];
  }
  const files = [];
  for (const entry of state.folder.files.values()) {
    if (isFogFilename(entry.name)) {
      files.push(entry);
    }
  }
  files.sort((a, b) => String(a.relPath).localeCompare(String(b.relPath), undefined, { sensitivity: "base" }));
  return files;
}

function isFogFilename(name) {
  const base = getFileBasename(name).toLowerCase();
  if (!base.endsWith(".cfg") && !base.endsWith(".dat")) {
    return false;
  }
  return /^(?:[0-3]_)?fog_level/.test(base);
}

async function onOpenSelectedFogFile() {
  const pakoReady = await ensurePakoLoaded();
  if (!pakoReady) {
    setStatus("Failed to load fog runtime (pako).", "error");
    return;
  }
  const key = String(els.fogFileSelect.value || "").toLowerCase();
  if (!key) {
    setStatus("Select a fog file first.", "error");
    return;
  }
  const entry = state.folder.files.get(key);
  if (!entry) {
    setStatus("Selected fog file was not found in loaded folder.", "error");
    return;
  }
  try {
    setStatus(`Opening fog file: ${entry.name}...`, "info");
    await openFogFromEntry(entry);
    setStatus(`Loaded fog file: ${entry.name}`, "success");
  } catch (error) {
    setStatus(`Failed to open fog file: ${error.message}`, "error");
  }
}

async function openFogFromEntry(entry, options = {}) {
  const parsed = await decodeFogFileRobust(entry.bytes);
  state.fog.currentKey = entry.key;
  state.fog.currentName = entry.name;
  state.fog.currentBytes = entry.bytes.slice();
  state.fog.currentPixels = parsed.pixels;
  state.fog.currentWidth = parsed.width;
  state.fog.currentHeight = parsed.height;
  state.fog.currentFooter = parsed.footer;
  state.fog.currentZlibOffset = parsed.zlibOffset;
  state.fog.currentZlibLength = parsed.zlibLength;
  state.fog.currentZlibHeader = parsed.zlibHeader || new Uint8Array([0x78, 0x9c]);
  state.fog.drawing = false;
  els.fogSaveBtn.disabled = false;
  els.fogDownloadCurrentBtn.disabled = false;
  els.fogEditorInfo.textContent = `${entry.name} • ${parsed.width}x${parsed.height}`;
  refreshFogFileList();
  renderFogCanvas();
  if (!options.silent) {
    updateFolderMeta();
  }
}

async function decodeFogFileRobust(fileBytes) {
  return decodeFogFile(fileBytes);
}

function decodeFogFile(fileBytes) {
  if (!window.pako || typeof window.pako.Inflate !== "function") {
    throw new Error("Pako library missing.");
  }
  const bytes = fileBytes instanceof Uint8Array ? fileBytes : new Uint8Array(fileBytes || []);
  for (let i = 0; i < bytes.length - 2; i += 1) {
    if (bytes[i] !== 0x78) {
      continue;
    }
    let inflated = null;
    try {
      inflated = inflateZlibPayload(bytes.subarray(i));
    } catch (_err) {
      inflated = null;
    }
    if (!inflated || !inflated.payload || inflated.payload.length < 8 || inflated.consumed <= 0) {
      continue;
    }
    const payload = inflated.payload;
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    const width = view.getUint32(0, true);
    const height = view.getUint32(4, true);
    if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {
      continue;
    }
    const pixelCount = width * height;
    const expected = 8 + pixelCount;
    if (expected > payload.length) {
      continue;
    }
    const filePixels = payload.slice(8, expected);
    const footer = payload.slice(expected);
    const pixels = flipVerticalGrayscale(filePixels, width, height);
    return {
      width,
      height,
      pixels,
      footer,
      zlibOffset: i,
      zlibLength: inflated.consumed,
      zlibHeader: bytes.slice(i, i + 2),
    };
  }
  throw new Error("Could not find a valid fog zlib stream.");
}

function inflateZlibPayload(inputBytes) {
  try {
    const inflator = new window.pako.Inflate();
    inflator.push(inputBytes, true);
    if (inflator.err) {
      throw new Error(inflator.msg || "inflate failed");
    }
    const payload = inflator.result instanceof Uint8Array
      ? inflator.result
      : (inflator.result ? new Uint8Array(inflator.result) : new Uint8Array(0));
    if (payload.length < 8) {
      throw new Error("inflate returned empty payload");
    }
    const consumed = detectZlibConsumedLength(inputBytes, payload, getPakoConsumedBytes(inflator, inputBytes.length));
    return { payload, consumed };
  } catch (zlibError) {
    // Some SnowRunner fog files decode with raw deflate when pako rejects the wrapper.
    if (!(inputBytes instanceof Uint8Array) || inputBytes.length < 6) {
      throw zlibError;
    }
    try {
      const rawPayload = window.pako.inflateRaw(inputBytes.subarray(2));
      const payload = rawPayload instanceof Uint8Array ? rawPayload : new Uint8Array(rawPayload || []);
      if (payload.length < 8) {
        throw new Error("inflateRaw returned empty payload");
      }
      const consumed = detectZlibConsumedLength(inputBytes, payload, inputBytes.length - 4);
      return { payload, consumed };
    } catch (_rawErr) {
      throw zlibError;
    }
  }
}

function getPakoConsumedBytes(inflator, inputLength) {
  if (!inflator || !inflator.strm) {
    return 0;
  }
  const strm = inflator.strm;
  if (typeof strm.total_in === "number" && Number.isFinite(strm.total_in) && strm.total_in > 0) {
    return strm.total_in;
  }
  if (typeof strm.next_in === "number" && Number.isFinite(strm.next_in) && strm.next_in > 0) {
    return strm.next_in;
  }
  if (
    typeof inputLength === "number" &&
    Number.isFinite(inputLength) &&
    typeof strm.avail_in === "number" &&
    Number.isFinite(strm.avail_in)
  ) {
    const consumed = inputLength - strm.avail_in;
    if (consumed > 0) {
      return consumed;
    }
  }
  return 0;
}

function detectZlibConsumedLength(inputBytes, payload, fallbackConsumed = 0) {
  const source = inputBytes instanceof Uint8Array ? inputBytes : new Uint8Array(inputBytes || []);
  const raw = payload instanceof Uint8Array ? payload : new Uint8Array(payload || []);
  if (fallbackConsumed > 0 && fallbackConsumed <= source.length) {
    return fallbackConsumed;
  }
  if (source.length >= 8 && raw.length > 0) {
    const checksum = adler32(raw);
    const end = source.length - 4;
    const b0 = (checksum >>> 24) & 0xff;
    const b1 = (checksum >>> 16) & 0xff;
    const b2 = (checksum >>> 8) & 0xff;
    const b3 = checksum & 0xff;
    if (source[end] === b0 && source[end + 1] === b1 && source[end + 2] === b2 && source[end + 3] === b3) {
      return source.length;
    }
    return source.length - 4;
  }
  return source.length;
}

function adler32(bytes) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes || []);
  let a = 1;
  let b = 0;
  const mod = 65521;
  for (let i = 0; i < data.length; i += 1) {
    a += data[i];
    if (a >= mod) {
      a -= mod;
    }
    b += a;
    b %= mod;
  }
  return ((b << 16) | a) >>> 0;
}

function renderFogCanvas() {
  const canvas = els.fogCanvas;
  if (!canvas) {
    return;
  }
  const ctx = canvas.getContext("2d");
  const width = Math.max(1, Math.floor(canvas.clientWidth || canvas.width || 1));
  const height = Math.max(1, Math.floor(canvas.clientHeight || canvas.height || 1));
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
  }
  ctx.fillStyle = "#161d1b";
  ctx.fillRect(0, 0, width, height);

  if (!state.fog.currentPixels || state.fog.currentWidth <= 0 || state.fog.currentHeight <= 0) {
    ctx.fillStyle = "rgba(255,255,255,0.72)";
    ctx.font = '14px "IBM Plex Mono", monospace';
    ctx.textAlign = "center";
    ctx.fillText("Open a fog file to start editing.", width / 2, height / 2);
    return;
  }

  const iw = state.fog.currentWidth;
  const ih = state.fog.currentHeight;
  const scale = Math.min(width / iw, height / ih);
  const drawW = Math.max(1, Math.floor(iw * scale));
  const drawH = Math.max(1, Math.floor(ih * scale));
  const offsetX = Math.floor((width - drawW) / 2);
  const offsetY = Math.floor((height - drawH) / 2);

  state.fog.viewScale = drawW / iw;
  state.fog.viewOffsetX = offsetX;
  state.fog.viewOffsetY = offsetY;

  const px = state.fog.currentPixels;
  const img = new ImageData(iw, ih);
  for (let i = 0, j = 0; i < px.length; i += 1, j += 4) {
    const v = px[i];
    img.data[j] = v;
    img.data[j + 1] = v;
    img.data[j + 2] = v;
    img.data[j + 3] = 255;
  }

  const offscreen = document.createElement("canvas");
  offscreen.width = iw;
  offscreen.height = ih;
  const offCtx = offscreen.getContext("2d");
  offCtx.putImageData(img, 0, 0);
  ctx.imageSmoothingEnabled = false;
  ctx.drawImage(offscreen, offsetX, offsetY, drawW, drawH);
  ctx.strokeStyle = "rgba(255,255,255,0.24)";
  ctx.lineWidth = 1;
  ctx.strokeRect(offsetX + 0.5, offsetY + 0.5, drawW - 1, drawH - 1);
}

function onFogPointerDown(event) {
  if (!state.fog.currentPixels) {
    return;
  }
  onFogBrushChanged();
  const pos = fogCanvasToImagePos(event, false);
  if (!pos) {
    return;
  }
  state.fog.drawing = true;
  state.fog.lastX = pos.x;
  state.fog.lastY = pos.y;
  paintFogStroke(pos.x, pos.y, pos.x, pos.y);
  renderFogCanvas();
  try {
    els.fogCanvas.setPointerCapture(event.pointerId);
  } catch (_err) {
    // ignore
  }
}

function onFogPointerMove(event) {
  if (!state.fog.drawing || !state.fog.currentPixels) {
    return;
  }
  const pos = fogCanvasToImagePos(event, true);
  if (!pos) {
    return;
  }
  paintFogStroke(state.fog.lastX, state.fog.lastY, pos.x, pos.y);
  state.fog.lastX = pos.x;
  state.fog.lastY = pos.y;
  renderFogCanvas();
}

function onFogPointerUp(event) {
  if (!state.fog.drawing) {
    return;
  }
  state.fog.drawing = false;
  try {
    els.fogCanvas.releasePointerCapture(event.pointerId);
  } catch (_err) {
    // ignore
  }
}

function fogCanvasToImagePos(event, clampToBounds) {
  const canvas = els.fogCanvas;
  if (!canvas || !state.fog.currentPixels) {
    return null;
  }
  const rect = canvas.getBoundingClientRect();
  const cx = event.clientX - rect.left;
  const cy = event.clientY - rect.top;
  const scale = state.fog.viewScale || 1;
  let x = Math.floor((cx - state.fog.viewOffsetX) / scale);
  let y = Math.floor((cy - state.fog.viewOffsetY) / scale);
  const iw = state.fog.currentWidth;
  const ih = state.fog.currentHeight;
  if (clampToBounds) {
    x = Math.max(0, Math.min(iw - 1, x));
    y = Math.max(0, Math.min(ih - 1, y));
    return { x, y };
  }
  if (x < 0 || y < 0 || x >= iw || y >= ih) {
    return null;
  }
  return { x, y };
}

function paintFogStroke(x0, y0, x1, y1) {
  const dx = x1 - x0;
  const dy = y1 - y0;
  const steps = Math.max(1, Math.floor(Math.hypot(dx, dy)));
  for (let i = 0; i <= steps; i += 1) {
    const t = i / steps;
    const x = Math.round(x0 + dx * t);
    const y = Math.round(y0 + dy * t);
    paintFogDot(x, y, state.fog.brushSize, state.fog.brushValue);
  }
}

function paintFogDot(cx, cy, size, value) {
  const pixels = state.fog.currentPixels;
  if (!pixels) {
    return;
  }
  const iw = state.fog.currentWidth;
  const ih = state.fog.currentHeight;
  const radius = Math.max(1, Math.floor(size / 2));
  const r2 = radius * radius;
  for (let dy = -radius; dy <= radius; dy += 1) {
    for (let dx = -radius; dx <= radius; dx += 1) {
      if ((dx * dx + dy * dy) > r2) {
        continue;
      }
      const x = cx + dx;
      const y = cy + dy;
      if (x < 0 || y < 0 || x >= iw || y >= ih) {
        continue;
      }
      pixels[y * iw + x] = value;
    }
  }
}

function onSaveCurrentFogToFolder() {
  if (!state.fog.currentPixels || !state.fog.currentKey) {
    setStatus("Open a fog file first.", "error");
    return;
  }
  const entry = state.folder.files.get(state.fog.currentKey);
  if (!entry) {
    setStatus("Current fog file is not part of the loaded folder.", "error");
    return;
  }
  try {
    setStatus(`Saving fog file: ${entry.name}...`, "info");
    const payload = buildCurrentFogPayload();
    const replaced = replaceFogPayloadInFile(
      state.fog.currentBytes,
      state.fog.currentZlibOffset,
      state.fog.currentZlibLength,
      state.fog.currentZlibHeader,
      payload,
    );
    entry.bytes = replaced.bytes;
    entry.dirty = true;
    state.fog.currentBytes = replaced.bytes;
    state.fog.currentZlibLength = replaced.zlibLength;
    updateFolderMeta();
    refreshFogFileList();
    setStatus(`Fog file saved: ${entry.name}`, "success");
  } catch (error) {
    setStatus(`Failed to save fog file: ${error.message}`, "error");
  }
}

function downloadCurrentFogFile() {
  if (!state.fog.currentBytes || !state.fog.currentName) {
    setStatus("Open a fog file first.", "error");
    return;
  }
  downloadBinary(state.fog.currentName, state.fog.currentBytes);
}

async function runFogAutomation(mode) {
  try {
    const pakoReady = await ensurePakoLoaded();
    if (!pakoReady) {
      setStatus("Failed to load fog runtime (pako).", "error");
      return;
    }
    if (!state.folder.loaded || state.folder.files.size === 0) {
      setStatus("Upload a save folder first.", "error");
      return;
    }
    const slot = Math.max(1, Math.min(4, Number.parseInt(String(els.fogSlotSelect.value || "1"), 10) || 1));
    const selectedOnly = String(els.fogFilterMode.value || "all") === "selected";
    const selectedCodes = [];
    for (const [code, input] of state.fog.seasonChecks.entries()) {
      if (input.checked) {
        selectedCodes.push(code);
      }
    }
    const extras = String(els.fogExtraSeasons.value || "")
      .split(",")
      .map((part) => part.trim())
      .filter((part) => /^\d+$/.test(part));

    const targets = [];
    for (const entry of getFogFolderEntries()) {
      if (parseFogSlot(entry.name) !== slot) {
        continue;
      }
      if (selectedOnly && !matchesFogRegionFilter(entry.name, selectedCodes, extras)) {
        continue;
      }
      targets.push(entry);
    }
    if (targets.length === 0) {
      const modeText = selectedOnly ? "selected filters" : `slot ${slot}`;
      els.fogAutoInfo.textContent = `No fog files matched ${modeText}.`;
      setStatus("No fog files matched automation filters.", "error");
      return;
    }

    const modeLabel = mode === "cover" ? "cover" : "uncover";
    els.fogAutoInfo.textContent = `Running ${modeLabel} on ${targets.length} fog file(s)...`;
    setStatus(`Fog automation started (${modeLabel}, ${targets.length} target files).`, "info");

    const fillValue = mode === "cover" ? 1 : 255;
    let processed = 0;
    let failed = 0;
    const touched = new Set();
    for (const entry of targets) {
      try {
        const parsed = await decodeFogFileRobust(entry.bytes);
        const painted = new Uint8Array(parsed.pixels.length);
        painted.fill(fillValue);
        const payload = buildFogPayload(parsed.width, parsed.height, painted, parsed.footer);
        const replaced = replaceFogPayloadInFile(entry.bytes, parsed.zlibOffset, parsed.zlibLength, parsed.zlibHeader, payload);
        entry.bytes = replaced.bytes;
        entry.dirty = true;
        processed += 1;
        touched.add(entry.key);
      } catch (_err) {
        failed += 1;
      }
    }
    updateFolderMeta();
    refreshFogFileList();

    if (state.fog.currentKey && touched.has(state.fog.currentKey)) {
      const currentEntry = state.folder.files.get(state.fog.currentKey);
      if (currentEntry) {
        try {
          await openFogFromEntry(currentEntry, { silent: true });
        } catch (_err) {
          // ignore refresh failure
        }
      }
    }

    els.fogAutoInfo.textContent = `Automation done: ${modeLabel} on ${processed} file(s)${failed ? `, failed: ${failed}` : ""}.`;
    setStatus(`Fog automation complete (${processed} updated, ${failed} failed).`, failed ? "info" : "success");
  } catch (error) {
    const message = error && error.message ? error.message : String(error || "Unknown error");
    if (els.fogAutoInfo) {
      els.fogAutoInfo.textContent = `Automation failed: ${message}`;
    }
    setStatus(`Fog automation failed: ${message}`, "error");
  }
}

function parseFogSlot(filename) {
  const base = getFileBasename(filename).toLowerCase();
  const prefixed = /^([0-3])_fog_level/.exec(base);
  if (prefixed) {
    return Number.parseInt(prefixed[1], 10) + 1;
  }
  if (/^fog_level/.test(base)) {
    return 1;
  }
  return -1;
}

function matchesFogRegionFilter(filename, selectedCodes, extraSeasonNumbers) {
  const lower = getFileBasename(filename).toLowerCase();
  for (const code of selectedCodes) {
    const token = `_${String(code).toLowerCase()}_`;
    if (lower.includes(token)) {
      return true;
    }
  }
  for (const num of extraSeasonNumbers) {
    const token = `_${num}_`;
    if (lower.includes(token)) {
      return true;
    }
  }
  return false;
}

function buildCurrentFogPayload() {
  return buildFogPayload(
    state.fog.currentWidth,
    state.fog.currentHeight,
    state.fog.currentPixels,
    state.fog.currentFooter,
  );
}

function buildFogPayload(width, height, editorPixels, footer) {
  const filePixels = flipVerticalGrayscale(editorPixels, width, height);
  const footerBytes = footer instanceof Uint8Array ? footer : new Uint8Array(footer || []);
  const out = new Uint8Array(8 + filePixels.length + footerBytes.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, width >>> 0, true);
  view.setUint32(4, height >>> 0, true);
  out.set(filePixels, 8);
  out.set(footerBytes, 8 + filePixels.length);
  return out;
}

function replaceFogPayloadInFile(originalBytes, zlibOffset, zlibLength, zlibHeader, payload) {
  const source = originalBytes instanceof Uint8Array ? originalBytes : new Uint8Array(originalBytes || []);
  const compressed = buildStoredZlibStream(payload, zlibHeader);
  const tailStart = zlibOffset + zlibLength;
  if (zlibOffset < 0 || zlibLength <= 0 || tailStart > source.length) {
    throw new Error("Invalid fog stream offsets.");
  }
  const out = new Uint8Array(zlibOffset + compressed.length + (source.length - tailStart));
  out.set(source.subarray(0, zlibOffset), 0);
  out.set(compressed, zlibOffset);
  out.set(source.subarray(tailStart), zlibOffset + compressed.length);
  return { bytes: out, zlibLength: compressed.length };
}

function buildStoredZlibStream(payload, headerBytes) {
  const input = payload instanceof Uint8Array ? payload : new Uint8Array(payload || []);
  const header = headerBytes instanceof Uint8Array && headerBytes.length >= 2
    ? headerBytes
    : new Uint8Array([0x78, 0x9c]);

  const chunks = [];
  chunks.push(header[0], header[1]);

  const maxChunk = 0xffff;
  let offset = 0;
  while (offset < input.length) {
    const left = input.length - offset;
    const size = left > maxChunk ? maxChunk : left;
    const bfinal = offset + size >= input.length ? 1 : 0;
    chunks.push(bfinal);
    chunks.push(size & 0xff, (size >>> 8) & 0xff);
    const nlen = 0xffff ^ size;
    chunks.push(nlen & 0xff, (nlen >>> 8) & 0xff);
    for (let i = 0; i < size; i += 1) {
      chunks.push(input[offset + i]);
    }
    offset += size;
  }

  const checksum = adler32(input);
  chunks.push((checksum >>> 24) & 0xff);
  chunks.push((checksum >>> 16) & 0xff);
  chunks.push((checksum >>> 8) & 0xff);
  chunks.push(checksum & 0xff);
  return new Uint8Array(chunks);
}

function flipVerticalGrayscale(pixels, width, height) {
  const src = pixels instanceof Uint8Array ? pixels : new Uint8Array(pixels || []);
  const out = new Uint8Array(src.length);
  const row = width;
  for (let y = 0; y < height; y += 1) {
    const srcPos = (height - 1 - y) * row;
    const dstPos = y * row;
    out.set(src.subarray(srcPos, srcPos + row), dstPos);
  }
  return out;
}

function decodeBytesToText(bytes) {
  return new TextDecoder("utf-8").decode(bytes);
}

function encodeTextToBytes(text) {
  return new TextEncoder().encode(String(text || ""));
}

function detectSingleFileRole(name, text) {
  const lowerName = String(name || "").toLowerCase();
  if (lowerName.startsWith("commonsslsave")) {
    return "common";
  }
  if (lowerName.startsWith("completesave")) {
    return "main";
  }
  const content = String(text || "");
  if (/"CommonSslSave"\s*:\s*\{/i.test(content) || /"SslValue"\s*:/i.test(content)) {
    return "common";
  }
  return "main";
}

function getImproveUploadEndpoint() {
  return String(IMPROVE_UPLOAD_ENDPOINT || "").trim();
}

function isImproveUploadEndpointConfigured(endpointOverride) {
  const endpoint = String(endpointOverride || getImproveUploadEndpoint()).trim();
  if (!/^https:\/\/[^ ]+$/i.test(endpoint)) {
    return false;
  }
  if (/your-worker\.workers\.dev/i.test(endpoint) || /example\.workers\.dev/i.test(endpoint)) {
    return false;
  }
  return true;
}

function updateImproveShareMeta(messageOverride) {
  if (!els.improveShareMeta) {
    return;
  }
  if (messageOverride) {
    els.improveShareMeta.textContent = String(messageOverride);
    return;
  }
  if (!els.improveShareCheckbox || !els.improveShareCheckbox.checked) {
    els.improveShareMeta.textContent = "Optional upload: off.";
    return;
  }
  if (!isImproveUploadEndpointConfigured()) {
    els.improveShareMeta.textContent = "Optional upload: enabled, but worker URL is not configured.";
    return;
  }
  els.improveShareMeta.textContent = "Optional upload: on. Matching files are sent anonymously when save data is loaded.";
}

function onImproveShareCheckboxChanged() {
  updateImproveShareMeta();
  if (!els.improveShareCheckbox || !els.improveShareCheckbox.checked) {
    return;
  }
  const standaloneEntries = getStandaloneLoadedEntriesForImproveShare();
  const folderEntries = state.folder.loaded && state.folder.files.size > 0
    ? [...state.folder.files.values()]
    : [];

  let entriesToUpload = [];
  if (state.improveShare.preferredSource === "single") {
    entriesToUpload = standaloneEntries.length > 0 ? standaloneEntries : folderEntries;
  } else if (state.improveShare.preferredSource === "folder") {
    entriesToUpload = folderEntries.length > 0 ? folderEntries : standaloneEntries;
  } else {
    entriesToUpload = standaloneEntries.length > 0 ? standaloneEntries : folderEntries;
  }

  if (entriesToUpload.length === 0) {
    updateImproveShareMeta("Optional upload: on. Upload a save folder or single file to send samples.");
    return;
  }
  Promise.resolve()
    .then(() => maybeUploadImproveSamples(entriesToUpload))
    .catch((error) => {
      const message = error && error.message ? error.message : String(error || "Unknown error");
      updateImproveShareMeta(`Optional upload failed: ${message}`);
      setStatus(`Optional upload failed: ${message}`, "error");
    });
}

function getStandaloneLoadedEntriesForImproveShare() {
  const out = [];
  if (state.main && !state.main.folderKey && state.main.name) {
    out.push({
      key: String(state.main.name).toLowerCase(),
      relPath: String(state.main.name),
      name: String(state.main.name),
      bytes: encodeTextToBytes(state.main.text),
      dirty: Boolean(state.main.dirty),
    });
  }
  if (state.common && !state.common.folderKey && state.common.name) {
    out.push({
      key: String(state.common.name).toLowerCase(),
      relPath: String(state.common.name),
      name: String(state.common.name),
      bytes: encodeTextToBytes(state.common.text),
      dirty: Boolean(state.common.dirty),
    });
  }
  return out;
}

function classifyImproveShareFileName(name) {
  const lower = String(name || "").toLowerCase();
  if (/^completesave\d*\.(cfg|dat)$/.test(lower) || /^commonsslsave\d*\.(cfg|dat)$/.test(lower)) {
    return "core";
  }
  if (/^(fog|field|sts|user|video|gamev)/.test(lower) || /^\d+_(fog|sts)/.test(lower)) {
    return "excluded";
  }
  return "unexpected";
}

function getImproveShareEntries(entriesOverride) {
  const source = Array.isArray(entriesOverride) ? entriesOverride : [...state.folder.files.values()];
  const core = [];
  const unexpected = [];
  let unexpectedTotalBytes = 0;

  for (const entry of source) {
    if (!entry || !entry.name || !(entry.bytes instanceof Uint8Array)) {
      continue;
    }
    const fileType = classifyImproveShareFileName(entry.name);
    if (fileType === "excluded") {
      continue;
    }
    if (fileType === "core") {
      core.push(entry);
      continue;
    }
    if (entry.bytes.length > IMPROVE_UPLOAD_UNEXPECTED_MAX_FILE_BYTES) {
      continue;
    }
    unexpected.push(entry);
    unexpectedTotalBytes += entry.bytes.length;
  }

  let selectedUnexpected = unexpected;
  if (
    unexpectedTotalBytes > IMPROVE_UPLOAD_UNEXPECTED_TOTAL_LIMIT_BYTES &&
    unexpected.length > IMPROVE_UPLOAD_UNEXPECTED_MAX_WHEN_TOTAL_EXCEEDED
  ) {
    selectedUnexpected = unexpected.slice(0, IMPROVE_UPLOAD_UNEXPECTED_MAX_WHEN_TOTAL_EXCEEDED);
  }

  return [...core, ...selectedUnexpected];
}

function getImproveShareSignature(entriesOverride) {
  const entries = getImproveShareEntries(entriesOverride);
  if (entries.length === 0) {
    return "";
  }
  const parts = entries
    .map((entry) => `${String(entry.key || entry.name || "").toLowerCase()}:${entry.bytes instanceof Uint8Array ? entry.bytes.length : 0}`)
    .sort();
  return `${String(state.folder.rootName || "").toLowerCase()}|${parts.join("|")}`;
}

async function maybeUploadImproveSamples(entriesOverride) {
  if (!els.improveShareCheckbox || !els.improveShareCheckbox.checked) {
    updateImproveShareMeta();
    return;
  }
  const endpoint = getImproveUploadEndpoint();
  if (!isImproveUploadEndpointConfigured(endpoint)) {
    const message = "Optional upload skipped: worker URL is not configured in app.js.";
    updateImproveShareMeta(message);
    setStatus(message, "error");
    return;
  }
  const sampleEntries = getImproveShareEntries(entriesOverride);
  if (sampleEntries.length === 0) {
    const message = "Optional upload skipped: no files matched upload rules in top folder.";
    updateImproveShareMeta(message);
    setStatus(message, "info");
    return;
  }

  const signature = getImproveShareSignature(sampleEntries);
  if (signature && state.improveShare.lastUploadedSignature === signature) {
    const message = "Optional upload already sent for this loaded folder.";
    updateImproveShareMeta(message);
    setStatus(message, "info");
    return;
  }
  if (state.improveShare.uploading) {
    const message = "Optional upload already in progress.";
    updateImproveShareMeta(message);
    setStatus(message, "info");
    return;
  }

  state.improveShare.uploading = true;
  updateImproveShareMeta(`Uploading anonymous samples (${sampleEntries.length} file(s))...`);
  setStatus(`Uploading anonymous samples (${sampleEntries.length} file(s))...`, "info");
  try {
    const result = await uploadImproveSamples(sampleEntries, endpoint);
    const batchId = String((result && (result.batchId || result.id)) || "").trim();
    const uploadedCountRaw = Number(result && result.uploadedCount);
    const uploadedCount = Number.isFinite(uploadedCountRaw) ? uploadedCountRaw : sampleEntries.length;
    const label = batchId ? `Optional upload complete (${uploadedCount} file(s), ID: ${batchId}).` : `Optional upload complete (${uploadedCount} file(s)).`;
    state.improveShare.lastUploadedSignature = signature;
    updateImproveShareMeta(label);
    setStatus(label, "success");
  } catch (error) {
    const message = error && error.message ? error.message : String(error || "Unknown error");
    updateImproveShareMeta(`Optional upload failed: ${message}`);
    setStatus(`Optional upload failed: ${message}`, "error");
  } finally {
    state.improveShare.uploading = false;
  }
}

async function uploadImproveSamples(entries, endpoint) {
  const target = String(endpoint || getImproveUploadEndpoint()).trim();
  if (!isImproveUploadEndpointConfigured(target)) {
    throw new Error("Worker URL is not configured.");
  }
  const formData = new FormData();
  let attached = 0;
  let skippedTooLarge = 0;
  for (const entry of entries || []) {
    if (!entry || !entry.name || !(entry.bytes instanceof Uint8Array)) {
      continue;
    }
    if (entry.bytes.length > IMPROVE_UPLOAD_MAX_FILE_BYTES) {
      skippedTooLarge += 1;
      continue;
    }
    const blob = new Blob([entry.bytes], { type: "application/octet-stream" });
    formData.append("files", blob, entry.name);
    attached += 1;
  }
  if (attached === 0) {
    throw new Error("No files matched upload rules.");
  }
  if (skippedTooLarge > 0) {
    formData.append("clientSkippedTooLarge", String(skippedTooLarge));
  }

  formData.append("source", "snowrunner-save-editor-web");
  formData.append("folderRoot", String(state.folder.rootName || ""));
  formData.append("uploadedAt", new Date().toISOString());

  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timeoutId = controller
    ? window.setTimeout(() => controller.abort(), IMPROVE_UPLOAD_TIMEOUT_MS)
    : null;
  try {
    const response = await fetch(target, {
      method: "POST",
      body: formData,
      signal: controller ? controller.signal : undefined,
    });
    const rawBody = await response.text();
    let payload = null;
    if (rawBody) {
      try {
        payload = JSON.parse(rawBody);
      } catch (_err) {
        // keep null
      }
    }
    if (!response.ok) {
      const reason = payload && payload.error ? String(payload.error) : rawBody || `HTTP ${response.status}`;
      throw new Error(reason);
    }
    if (!payload || payload.success === false) {
      const reason = payload && payload.error ? String(payload.error) : "Worker did not return success JSON.";
      throw new Error(reason);
    }
    return payload;
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error("Request timed out.");
    }
    throw error;
  } finally {
    if (timeoutId !== null) {
      window.clearTimeout(timeoutId);
    }
  }
}

function getMainFolderEntries(entriesOverride) {
  const source = Array.isArray(entriesOverride) ? entriesOverride : [...state.folder.files.values()];
  const out = source.filter((entry) => {
    const lower = String(entry && entry.name ? entry.name : "").toLowerCase();
    return /^completesave\d*\.(cfg|dat)$/.test(lower);
  });
  out.sort((a, b) => {
    const ai = extractCompleteSaveIndex(a.name);
    const bi = extractCompleteSaveIndex(b.name);
    if (ai !== bi) {
      return ai - bi;
    }
    const aCfg = String(a.name || "").toLowerCase().endsWith(".cfg");
    const bCfg = String(b.name || "").toLowerCase().endsWith(".cfg");
    if (aCfg !== bCfg) {
      return aCfg ? -1 : 1;
    }
    return String(a.name || "").localeCompare(String(b.name || ""), undefined, { sensitivity: "base" });
  });
  return out;
}

function extractCompleteSaveIndex(name) {
  const m = /^completesave(\d*)\.(?:cfg|dat)$/i.exec(String(name || ""));
  if (!m) {
    return Number.MAX_SAFE_INTEGER;
  }
  if (!m[1]) {
    return 0;
  }
  const parsed = Number.parseInt(m[1], 10);
  return Number.isFinite(parsed) ? parsed : Number.MAX_SAFE_INTEGER;
}

function pickCommonEntryFromFolder(entries) {
  for (const entry of entries) {
    const lower = String(entry.name || "").toLowerCase();
    if (lower.startsWith("commonsslsave") && (lower.endsWith(".cfg") || lower.endsWith(".dat"))) {
      return entry;
    }
  }
  return null;
}

function detectFolderRoot(relativePaths) {
  const paths = (relativePaths || []).filter(Boolean).map((item) => String(item).replace(/\\/g, "/"));
  if (paths.length === 0) {
    return "";
  }
  const first = paths[0].split("/")[0];
  if (!first) {
    return "";
  }
  for (const path of paths) {
    if (!path.startsWith(`${first}/`) && path !== first) {
      return "";
    }
  }
  return first;
}

function getFileBasename(path) {
  const normalized = String(path || "").replace(/\\/g, "/");
  const parts = normalized.split("/");
  return parts[parts.length - 1] || normalized;
}

function isTopLevelFolderFile(relativePath) {
  const normalized = String(relativePath || "").replace(/\\/g, "/");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length <= 1) {
    return true;
  }
  // webkitdirectory usually yields: rootFolder/file.ext
  return parts.length === 2;
}

function renderTrialsList() {
  els.trialsList.innerHTML = "";
  state.trialChecks.clear();
  for (const [name, code] of TRIALS_LIST) {
    const id = `trial-${code.toLowerCase()}`;
    const box = makeCheckbox(id, name);
    state.trialChecks.set(code, box.input);
    els.trialsList.append(box.label);
  }
}

function setAllTrials(value) {
  for (const input of state.trialChecks.values()) {
    input.checked = value;
  }
}

function buildRulesEditor() {
  if (!els.rulesEditor) {
    return;
  }
  els.rulesEditor.innerHTML = "";
  state.rules.controls.clear();
  for (const def of RULE_DEFINITIONS) {
    const row = document.createElement("div");
    row.className = "rule-row";
    const label = document.createElement("label");
    label.textContent = def.label;

    const select = document.createElement("select");
    const options = def.key === "gameDifficultyMode"
      ? def.options
      : [...def.options, { label: RULE_RANDOM_LABEL, value: RULE_RANDOM_VALUE }];
    for (const opt of options) {
      const option = document.createElement("option");
      option.value = opt.label;
      option.textContent = opt.label;
      select.append(option);
    }
    select.addEventListener("change", () => onRuleDropdownChanged(def));
    row.append(label, select);
    els.rulesEditor.append(row);
    state.rules.controls.set(def.key, select);
  }
}

function getRuleDefaultLabel(def) {
  return def && def.options && def.options[0] ? def.options[0].label : "";
}

function getRuleOptionByLabel(def, label) {
  return (def && Array.isArray(def.options) ? def.options : []).find((opt) => opt.label === label) || null;
}

function getRuleOptionByValue(def, value) {
  return (def && Array.isArray(def.options) ? def.options : []).find((opt) => Object.is(opt.value, value)) || null;
}

function setRuleSelectLabel(key, label) {
  const select = state.rules.controls.get(key);
  if (!select || !label) {
    return;
  }
  if ([...select.options].some((option) => option.value === label)) {
    select.value = label;
  }
}

function setGameDifficultyRuleLabel(label) {
  setRuleSelectLabel("gameDifficultyMode", label);
}

function resetRulesUiToDefaults() {
  for (const def of RULE_DEFINITIONS) {
    setRuleSelectLabel(def.key, getRuleDefaultLabel(def));
  }
}

function setAllRulesToRandom() {
  for (const def of RULE_DEFINITIONS) {
    if (def.key === "gameDifficultyMode") {
      continue;
    }
    setRuleSelectLabel(def.key, RULE_RANDOM_LABEL);
  }
  setGameDifficultyRuleLabel("New Game+");
}

function onRulesRandomToggleChanged() {
  if (els.rulesRandom && els.rulesRandom.checked) {
    setAllRulesToRandom();
  }
}

function onRuleDropdownChanged(def) {
  if (!def) {
    return;
  }
  const select = state.rules.controls.get(def.key);
  const selectedLabel = select && select.value ? select.value : getRuleDefaultLabel(def);
  if (def.key === "gameDifficultyMode") {
    if (selectedLabel === "Normal") {
      if (els.rulesRandom) {
        els.rulesRandom.checked = false;
      }
      resetRulesUiToDefaults();
      setGameDifficultyRuleLabel("Normal");
    } else if (selectedLabel === "Hard" && els.rulesRandom) {
      els.rulesRandom.checked = false;
    }
    return;
  }
  setGameDifficultyRuleLabel("New Game+");
}

function loadSettingsDictionary(content, fallback) {
  const defaults = fallback && typeof fallback === "object" && !Array.isArray(fallback) ? fallback : {};
  const match = new RegExp(`"${escapeRegExp("settingsDictionaryForNGPScreen")}"\\s*:\\s*\\{`, "i").exec(content || "");
  if (!match) {
    return { ...defaults };
  }
  try {
    const block = extractBraceBlock(content, match.index);
    const parsed = JSON.parse(block.block);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ...defaults };
    }
    return { ...defaults, ...parsed };
  } catch (_error) {
    return { ...defaults };
  }
}

function resolveRandomRuleLabel(def) {
  const concrete = (def.options || []).map((opt) => opt.label).filter((label) => label && label !== RULE_RANDOM_LABEL);
  if (concrete.length === 0) {
    return getRuleDefaultLabel(def);
  }
  const index = Math.floor(Math.random() * concrete.length);
  return concrete[index] || getRuleDefaultLabel(def);
}

function inferRuleLabelFromContent(def, content, settingsDict) {
  const defaultLabel = getRuleDefaultLabel(def);
  const meta = RULE_NGP_DICT_META[def.key];
  if (meta && settingsDict && Object.prototype.hasOwnProperty.call(settingsDict, meta.ngpKey)) {
    const stateValue = Number.parseInt(String(settingsDict[meta.ngpKey]), 10);
    if (Number.isFinite(stateValue)) {
      for (const [label, mappedState] of Object.entries(meta.labelToState || {})) {
        if (Number.parseInt(String(mappedState), 10) === stateValue && getRuleOptionByLabel(def, label)) {
          return label;
        }
      }
    }
  }

  if (!content) {
    return defaultLabel;
  }

  if (def.key === "truckAvailability") {
    const availability = readSimpleValueKey(content, "truckAvailability");
    if (availability === 2) {
      const level = readSimpleValueKey(content, "truckAvailabilityLevel");
      if (typeof level === "number" && level >= 30 && getRuleOptionByLabel(def, "store unlocks at rank 30")) {
        return "store unlocks at rank 30";
      }
      if (typeof level === "number" && level >= 20 && getRuleOptionByLabel(def, "store unlocks at rank 20")) {
        return "store unlocks at rank 20";
      }
      if (getRuleOptionByLabel(def, "store unlocks at rank 10")) {
        return "store unlocks at rank 10";
      }
    }
  }

  if (def.key === "internalAddonAvailability") {
    const addonAvailability = readSimpleValueKey(content, "internalAddonAvailability");
    if (addonAvailability === 2) {
      const amount = readSimpleValueKey(content, "internalAddonAmount");
      if (typeof amount === "number") {
        if (amount >= 10 && amount <= 50 && getRuleOptionByLabel(def, "10-50 per garage")) {
          return "10-50 per garage";
        }
        if (amount >= 30 && amount <= 100 && getRuleOptionByLabel(def, "30-100 per garage")) {
          return "30-100 per garage";
        }
        if (amount >= 50 && amount <= 150 && getRuleOptionByLabel(def, "50-150 per garage")) {
          return "50-150 per garage";
        }
        if (amount >= 0 && amount <= 100 && getRuleOptionByLabel(def, "0-100 per garage")) {
          return "0-100 per garage";
        }
      }
    }
  }

  if (def.key === "maxContestAttempts") {
    if (readSimpleValueKey(content, "isGoldFailReason") === true && getRuleOptionByLabel(def, "gold time only")) {
      return "gold time only";
    }
  }

  if (def.key === "regionRepaireMoneyFactor") {
    const moneyFactor = readSimpleValueKey(content, "regionRepaireMoneyFactor");
    const pointsFactor = readSimpleValueKey(content, "regionRepairePointsFactor");
    if (moneyFactor !== undefined && pointsFactor !== undefined) {
      for (const opt of def.options) {
        if (Object.is(opt.value, moneyFactor) && Object.is(opt.value, pointsFactor)) {
          return opt.label;
        }
      }
    }
  }

  if (def.key === "needToAddDlcTrucks") {
    const dlcRaw = readSimpleValueKey(content, "needToAddDlcTrucks");
    const fallbackRaw = dlcRaw === undefined ? readSimpleValueKey(content, "isDLCVehiclesAvailable") : dlcRaw;
    const found = getRuleOptionByValue(def, fallbackRaw);
    return found ? found.label : defaultLabel;
  }

  const current = readSimpleValueKey(content, def.key);
  const found = getRuleOptionByValue(def, current);
  return found ? found.label : defaultLabel;
}

function hydrateRulesFromMain() {
  if (!state.rules.controls.size) {
    return;
  }
  const settingsDict = state.main ? loadSettingsDictionary(state.main.text, DEFAULT_SETTINGS_DICT) : { ...DEFAULT_SETTINGS_DICT };
  for (const def of RULE_DEFINITIONS) {
    const select = state.rules.controls.get(def.key);
    if (!select) {
      continue;
    }
    const label = state.main ? inferRuleLabelFromContent(def, state.main.text, settingsDict) : getRuleDefaultLabel(def);
    select.value = label;
  }
}

function onApplyRules() {
  if (!requireMain()) {
    return;
  }
  try {
    if (els.rulesRandom && els.rulesRandom.checked) {
      setAllRulesToRandom();
    }
    const resolvedLabels = new Map();
    for (const def of RULE_DEFINITIONS) {
      const select = state.rules.controls.get(def.key);
      const rawLabel = select && select.value ? select.value : getRuleDefaultLabel(def);
      const label = rawLabel === RULE_RANDOM_LABEL ? resolveRandomRuleLabel(def) : rawLabel;
      resolvedLabels.set(def.key, label || getRuleDefaultLabel(def));
    }

    const difficultyLabel = resolvedLabels.get("gameDifficultyMode") || "Normal";
    if (difficultyLabel === "Normal") {
      for (const def of RULE_DEFINITIONS) {
        if (def.key === "gameDifficultyMode") {
          continue;
        }
        resolvedLabels.set(def.key, getRuleDefaultLabel(def));
      }
    }

    for (const def of RULE_DEFINITIONS) {
      const select = state.rules.controls.get(def.key);
      if (select) {
        select.value = resolvedLabels.get(def.key) || getRuleDefaultLabel(def);
      }
    }

    let content = state.main.text;
    for (const def of RULE_DEFINITIONS) {
      const label = resolvedLabels.get(def.key) || getRuleDefaultLabel(def);
      const option = getRuleOptionByLabel(def, label) || def.options[0];
      const value = option ? option.value : 0;
      if (typeof value === "boolean") {
        content = replaceOrInsertBoolean(content, def.key, value);
      } else {
        content = replaceOrInsertNumeric(content, def.key, value);
      }
    }

    const difficultyOption = getRuleOptionByLabel(
      RULE_DEFINITIONS.find((def) => def.key === "gameDifficultyMode"),
      resolvedLabels.get("gameDifficultyMode") || "Normal",
    );
    const gameDifficulty = Number.parseInt(String(difficultyOption ? difficultyOption.value : 0), 10);
    content = replaceOrInsertBoolean(content, "isHardMode", Number.isFinite(gameDifficulty) && gameDifficulty === 1);

    const truckAvailabilityLabel = resolvedLabels.get("truckAvailability");
    if (truckAvailabilityLabel === "store unlocks at rank 10") {
      content = replaceOrInsertNumeric(content, "truckAvailabilityLevel", 10);
    } else if (truckAvailabilityLabel === "store unlocks at rank 20") {
      content = replaceOrInsertNumeric(content, "truckAvailabilityLevel", 20);
    } else if (truckAvailabilityLabel === "store unlocks at rank 30") {
      content = replaceOrInsertNumeric(content, "truckAvailabilityLevel", 30);
    }

    const internalAddonLabel = resolvedLabels.get("internalAddonAvailability");
    if (Object.prototype.hasOwnProperty.call(INTERNAL_ADDON_AMOUNT_BY_LABEL, internalAddonLabel)) {
      content = replaceOrInsertNumeric(content, "internalAddonAmount", INTERNAL_ADDON_AMOUNT_BY_LABEL[internalAddonLabel]);
    }

    const maxContestLabel = resolvedLabels.get("maxContestAttempts");
    content = replaceOrInsertBoolean(content, "isGoldFailReason", maxContestLabel === "gold time only");
    if (maxContestLabel === "gold time only") {
      content = replaceOrInsertNumeric(content, "maxContestAttempts", -1);
    }

    const regionRepairDef = RULE_DEFINITIONS.find((def) => def.key === "regionRepaireMoneyFactor");
    const regionRepairOption = getRuleOptionByLabel(regionRepairDef, resolvedLabels.get("regionRepaireMoneyFactor"));
    if (regionRepairOption) {
      content = replaceOrInsertNumeric(content, "regionRepairePointsFactor", regionRepairOption.value);
    }

    const dlcDef = RULE_DEFINITIONS.find((def) => def.key === "needToAddDlcTrucks");
    const dlcOption = getRuleOptionByLabel(dlcDef, resolvedLabels.get("needToAddDlcTrucks"));
    if (dlcOption) {
      content = replaceOrInsertBoolean(content, "isDLCVehiclesAvailable", Boolean(dlcOption.value));
    }

    content = sanitizeRulesContent(content, resolvedLabels, difficultyLabel);
    commitMain(content, "Rules updated.");
  } catch (error) {
    setStatus(`Failed to apply rules: ${error.message}`, "error");
  }
}

function sanitizeRulesContent(content, resolvedLabels, difficultyLabel) {
  let out = content;
  for (const def of RULE_DEFINITIONS) {
    const safeDefault = def.options[0] ? def.options[0].value : 0;
    out = ensureKeyWithDefaultLiteral(out, def.key, safeDefault, false);
  }
  out = ensureKeyWithDefaultLiteral(out, "autoloadPrice", DEFAULT_AUTOLOAD_PRICE, true);
  out = ensureArrayKeyWithDefault(out, "recoveryPrice", DEFAULT_RECOVERY_PRICE);
  out = ensureArrayKeyWithDefault(out, "fullRepairPrice", DEFAULT_FULL_REPAIR_PRICE);
  out = ensureSettingsDictionaryForNgp(out, DEFAULT_SETTINGS_DICT);
  let settingsDict = difficultyLabel === "Normal"
    ? { ...DEFAULT_SETTINGS_DICT }
    : loadSettingsDictionary(out, DEFAULT_SETTINGS_DICT);
  for (const def of RULE_DEFINITIONS) {
    const meta = RULE_NGP_DICT_META[def.key];
    if (!meta) {
      continue;
    }
    const label = resolvedLabels instanceof Map
      ? (resolvedLabels.get(def.key) || getRuleDefaultLabel(def))
      : getRuleDefaultLabel(def);
    if (Object.prototype.hasOwnProperty.call(meta.labelToState, label)) {
      settingsDict[meta.ngpKey] = meta.labelToState[label];
    }
  }
  out = replaceOrInsertJsonLiteral(out, "settingsDictionaryForNGPScreen", JSON.stringify(settingsDict));
  out = ensureDeployPriceObject(out, DEFAULT_DEPLOY_PRICE);
  return out;
}

function ensureKeyWithDefaultLiteral(content, key, value, treatZeroAsMissing) {
  const jsonValue = JSON.stringify(value);
  const nullRe = new RegExp(`("${escapeRegExp(key)}"\\s*:\\s*)null`, "gi");
  let out = content.replace(nullRe, (_, p1) => `${p1}${jsonValue}`);
  if (treatZeroAsMissing) {
    const zeroRe = new RegExp(`("${escapeRegExp(key)}"\\s*:\\s*)0\\b`, "gi");
    out = out.replace(zeroRe, (_, p1) => `${p1}${jsonValue}`);
  }
  const existsRe = new RegExp(`"${escapeRegExp(key)}"\\s*:`, "i");
  if (!existsRe.test(out)) {
    out = insertKeyAtRoot(out, key, jsonValue);
  }
  return out;
}

function ensureArrayKeyWithDefault(content, key, defaultList) {
  const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(\\[[^\\]]*\\])`, "i");
  const m = re.exec(content);
  if (!m) {
    return replaceOrInsertJsonLiteral(content, key, JSON.stringify(defaultList));
  }
  try {
    const arr = JSON.parse(m[1]);
    let invalid = !Array.isArray(arr) || arr.length < defaultList.length;
    if (!invalid) {
      for (let i = 2; i < defaultList.length; i += 1) {
        const val = arr[i];
        if (typeof val !== "number" || Number.isNaN(val) || val === 0) {
          invalid = true;
          break;
        }
      }
    }
    if (invalid) {
      return replaceOrInsertJsonLiteral(content, key, JSON.stringify(defaultList));
    }
    return content;
  } catch (_error) {
    return replaceOrInsertJsonLiteral(content, key, JSON.stringify(defaultList));
  }
}

function ensureSettingsDictionaryForNgp(content, defaultDict) {
  const key = "settingsDictionaryForNGPScreen";
  let out = content;
  const defaultText = JSON.stringify(defaultDict);
  const nullOrZeroRe = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(null|0\\b)`, "gi");
  out = out.replace(nullOrZeroRe, `"${key}":${defaultText}`);

  const objectRe = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(\\{[^\\}]*\\})`, "i");
  const m = objectRe.exec(out);
  if (!m) {
    return replaceOrInsertJsonLiteral(out, key, defaultText);
  }
  try {
    const parsed = JSON.parse(m[1]);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return replaceOrInsertJsonLiteral(out, key, defaultText);
    }
    return out;
  } catch (_error) {
    return replaceOrInsertJsonLiteral(out, key, defaultText);
  }
}

function ensureDeployPriceObject(content, defaultValue) {
  const key = "deployPrice";
  const defaultText = JSON.stringify(defaultValue);
  const objectRe = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(\\{[^\\}]*\\})`, "i");
  const m = objectRe.exec(content);
  if (!m) {
    return replaceOrInsertJsonLiteral(content, key, defaultText);
  }
  try {
    const parsed = JSON.parse(m[1]);
    if (
      !parsed ||
      typeof parsed !== "object" ||
      Array.isArray(parsed) ||
      !Object.prototype.hasOwnProperty.call(parsed, "Region") ||
      !Object.prototype.hasOwnProperty.call(parsed, "Map")
    ) {
      return replaceOrInsertJsonLiteral(content, key, defaultText);
    }
    return content;
  } catch (_error) {
    return replaceOrInsertJsonLiteral(content, key, defaultText);
  }
}

function refreshGameStatsEditor() {
  if (!state.main) {
    if (els.gameStatsInfo) {
      els.gameStatsInfo.textContent = "No file loaded.";
    }
    if (els.distanceList) {
      els.distanceList.innerHTML = "";
    }
    if (els.gameStatList) {
      els.gameStatList.innerHTML = "";
    }
    state.gameStats.distanceInputs.clear();
    state.gameStats.statInputs.clear();
    return;
  }

  try {
    const parsed = parseGameStatsData(state.main.text);
    state.gameStats.distanceInputs.clear();
    state.gameStats.statInputs.clear();
    els.distanceList.innerHTML = "";
    els.gameStatList.innerHTML = "";

    for (const [key, value] of parsed.distanceEntries) {
      const row = createStatRow(key, value, prettifyDistanceName(key));
      els.distanceList.append(row.row);
      state.gameStats.distanceInputs.set(key, row.input);
    }

    for (const [key, value] of parsed.statEntries) {
      const row = createStatRow(key, value, prettifyGameStatName(key));
      els.gameStatList.append(row.row);
      state.gameStats.statInputs.set(key, row.input);
    }

    els.gameStatsInfo.textContent = `Distance entries: ${parsed.distanceEntries.length} | Game stats entries: ${parsed.statEntries.length}`;
  } catch (error) {
    els.gameStatsInfo.textContent = `Failed to parse game stats: ${error.message}`;
    els.distanceList.innerHTML = "";
    els.gameStatList.innerHTML = "";
    state.gameStats.distanceInputs.clear();
    state.gameStats.statInputs.clear();
  }
}

function parseGameStatsData(content) {
  let gameStatObj = {};
  let gameStatBlock = null;
  const mStat = /"gameStat"\s*:\s*\{/i.exec(content);
  if (mStat) {
    gameStatBlock = extractBraceBlock(content, mStat.index);
    gameStatObj = JSON.parse(gameStatBlock.block);
    if (!gameStatObj || typeof gameStatObj !== "object" || Array.isArray(gameStatObj)) {
      gameStatObj = {};
    }
  }

  const distBlock = findBestDistanceBlock(content);
  let distanceObj = {};
  if (distBlock) {
    distanceObj = distBlock.parsed;
  }

  return {
    gameStatBlock,
    distanceBlock: distBlock,
    statObj: gameStatObj,
    distanceObj,
    statEntries: Object.entries(gameStatObj).sort((a, b) => String(a[0]).localeCompare(String(b[0]))),
    distanceEntries: Object.entries(distanceObj).sort((a, b) => compareDistanceKey(a[0], b[0])),
  };
}

function findBestDistanceBlock(content) {
  const matches = [...content.matchAll(/"distance"\s*:\s*\{/gi)];
  let best = null;
  let bestKnownCount = -1;
  let bestTotalCount = -1;
  for (const hit of matches) {
    try {
      const block = extractBraceBlock(content, hit.index);
      const parsed = JSON.parse(block.block);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        continue;
      }
      const knownCount = countKnownDistanceKeys(parsed);
      const totalCount = Object.keys(parsed).length;
      if (knownCount > bestKnownCount || (knownCount === bestKnownCount && totalCount > bestTotalCount)) {
        bestKnownCount = knownCount;
        bestTotalCount = totalCount;
        best = { ...block, parsed };
      }
    } catch (_err) {
      // skip invalid blocks
    }
  }
  return best;
}

function countKnownDistanceKeys(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return 0;
  }
  let count = 0;
  for (const key of Object.keys(parsed)) {
    const up = String(key || "").toUpperCase();
    if (REGION_ORDER.includes(up) || up === "TRIALS") {
      count += 1;
    }
  }
  return count;
}

function compareDistanceKey(a, b) {
  const aUp = String(a || "").toUpperCase();
  const bUp = String(b || "").toUpperCase();
  const aIdx = REGION_ORDER.indexOf(aUp);
  const bIdx = REGION_ORDER.indexOf(bUp);
  if (aIdx >= 0 && bIdx >= 0) {
    return aIdx - bIdx;
  }
  if (aIdx >= 0) {
    return -1;
  }
  if (bIdx >= 0) {
    return 1;
  }
  return aUp.localeCompare(bUp);
}

function prettifyGameStatName(rawKey) {
  const words = String(rawKey || "")
    .split("_")
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());
  const pretty = words.join(" ");
  const replacements = {
    "Truck Sold": "Trucks Sold",
    "Truck Bought": "Trucks Bought",
    "Trailer Sold": "Trailers Sold",
    "Trailer Bought": "Trailers Bought",
    "Addon Sold": "Addons Sold",
    "Addon Bought": "Addons Bought",
  };
  return replacements[pretty] || pretty || String(rawKey || "");
}

function prettifyDistanceName(rawKey) {
  const up = String(rawKey || "").toUpperCase();
  return REGION_LONG_NAME_MAP[up] || String(rawKey || "");
}

function createStatRow(key, value, labelText) {
  const row = document.createElement("div");
  row.className = "stat-row";
  const label = document.createElement("span");
  label.textContent = labelText || key;
  const input = document.createElement("input");
  input.type = "text";
  input.value = value == null ? "" : String(value);
  row.append(label, input);
  return { row, input };
}

function saveGameStatsToMain() {
  if (!requireMain()) {
    return;
  }
  try {
    const parsed = parseGameStatsData(state.main.text);
    const updates = [];

    if (parsed.gameStatBlock) {
      for (const [key, input] of state.gameStats.statInputs.entries()) {
        const oldValue = parsed.statObj[key];
        parsed.statObj[key] = parseStatInputValue(input.value, oldValue);
      }
      updates.push({
        start: parsed.gameStatBlock.start,
        end: parsed.gameStatBlock.end,
        value: JSON.stringify(parsed.statObj),
      });
    }

    if (parsed.distanceBlock) {
      for (const [key, input] of state.gameStats.distanceInputs.entries()) {
        const oldValue = parsed.distanceObj[key];
        parsed.distanceObj[key] = parseStatInputValue(input.value, oldValue);
      }
      updates.push({
        start: parsed.distanceBlock.start,
        end: parsed.distanceBlock.end,
        value: JSON.stringify(parsed.distanceObj),
      });
    }

    if (updates.length === 0) {
      setStatus("No gameStat/distance blocks found to save.", "error");
      return;
    }

    updates.sort((a, b) => b.start - a.start);
    let content = state.main.text;
    for (const item of updates) {
      content = content.slice(0, item.start) + item.value + content.slice(item.end);
    }
    commitMain(content, "Game stats saved.");
  } catch (error) {
    setStatus(`Failed to save game stats: ${error.message}`, "error");
  }
}

function parseStatInputValue(raw, oldValue) {
  const txt = String(raw == null ? "" : raw).trim();
  if (!txt) {
    return oldValue;
  }
  if (typeof oldValue === "number") {
    const num = parseStrictNumber(txt);
    return num != null ? num : oldValue;
  }
  if (typeof oldValue === "boolean") {
    if (txt.toLowerCase() === "true") {
      return true;
    }
    if (txt.toLowerCase() === "false") {
      return false;
    }
    return oldValue;
  }
  if (/^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(txt)) {
    const num = parseStrictNumber(txt);
    return num != null ? num : txt;
  }
  return txt;
}

function renderRankXpTable() {
  if (!els.rankXpTable) {
    return;
  }
  els.rankXpTable.innerHTML = "";
  for (const rank of Object.keys(RANK_XP_REQUIREMENTS).map(Number).sort((a, b) => a - b)) {
    const row = document.createElement("div");
    row.className = "rank-xp-row";
    const left = document.createElement("span");
    left.textContent = String(rank);
    const colon = document.createElement("span");
    colon.textContent = ":";
    const right = document.createElement("span");
    right.textContent = String(RANK_XP_REQUIREMENTS[rank]);
    row.append(left, colon, right);
    els.rankXpTable.append(row);
  }
}

function xpForRank(rank) {
  return RANK_XP_REQUIREMENTS[rank] != null ? Number(RANK_XP_REQUIREMENTS[rank]) : null;
}

function rankForXp(xp) {
  let out = 1;
  for (const rank of Object.keys(RANK_XP_REQUIREMENTS).map(Number).sort((a, b) => a - b)) {
    if (xp >= RANK_XP_REQUIREMENTS[rank]) {
      out = rank;
    }
  }
  return out;
}

function syncXpFromRankInput() {
  if (state.ui.rankXpSyncLock) {
    return;
  }
  const parsed = parseOptionalStrictInt(els.rankInput.value);
  if (parsed.error) {
    return;
  }
  const rank = parsed.value;
  if (rank == null || rank < 1 || rank > 30) {
    return;
  }
  const xp = xpForRank(rank);
  if (xp == null) {
    return;
  }
  state.ui.rankXpSyncLock = true;
  try {
    els.xpInput.value = String(xp);
  } finally {
    state.ui.rankXpSyncLock = false;
  }
}

function syncRankFromXpInput() {
  if (state.ui.rankXpSyncLock) {
    return;
  }
  const parsed = parseOptionalStrictInt(els.xpInput.value);
  if (parsed.error) {
    return;
  }
  const xp = parsed.value;
  if (xp == null || xp < 0) {
    return;
  }
  const rank = rankForXp(xp);
  state.ui.rankXpSyncLock = true;
  try {
    els.rankInput.value = String(rank);
  } finally {
    state.ui.rankXpSyncLock = false;
  }
}

function refreshObjectivesFromMain() {
  if (!state.main) {
    state.objectives.data = null;
    state.objectives.visibleKeys = [];
    state.objectives.selected.clear();
    state.objectives.finishedInSave.clear();
    renderObjectivesList();
    return;
  }
  try {
    const m = /"objectiveStates"\s*:\s*\{/i.exec(state.main.text);
    if (!m) {
      state.objectives.data = null;
      state.objectives.visibleKeys = [];
      state.objectives.selected.clear();
      state.objectives.finishedInSave.clear();
      els.objectivesInfo.textContent = "objectiveStates block not found.";
      els.objectivesList.innerHTML = "";
      return;
    }
    const block = extractBraceBlock(state.main.text, m.index);
    const parsed = JSON.parse(block.block);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("objectiveStates is not an object.");
    }
    state.objectives.data = parsed;
    state.objectives.finishedInSave = collectFinishedObjectivesFromMain(state.main.text, parsed);
    state.objectives.selected.clear();
    renderObjectivesList();
  } catch (error) {
    state.objectives.data = null;
    state.objectives.visibleKeys = [];
    state.objectives.selected.clear();
    state.objectives.finishedInSave.clear();
    els.objectivesInfo.textContent = `Failed to parse objectiveStates: ${error.message}`;
    els.objectivesList.innerHTML = "";
  }
}

function collectFinishedObjectivesFromMain(content, objectiveStates) {
  const finished = new Set();

  if (objectiveStates && typeof objectiveStates === "object" && !Array.isArray(objectiveStates)) {
    for (const [key, entry] of Object.entries(objectiveStates)) {
      if (isObjectiveFinishedEntry(entry)) {
        finished.add(String(key || "").trim().toUpperCase());
      }
    }
  }

  const re = /"(CompleteSave\d*)"\s*:\s*\{/g;
  let hit = null;
  while ((hit = re.exec(content)) !== null) {
    try {
      const block = extractBraceBlock(content, hit.index);
      const valueData = JSON.parse(block.block);

      let ssl = valueData.SslValue;
      if (!ssl || typeof ssl !== "object") {
        const nested = valueData[hit[1]];
        ssl = nested && typeof nested === "object" ? nested.SslValue : null;
      }
      if (!ssl || typeof ssl !== "object") {
        continue;
      }

      const finishedRaw = ssl.finishedObjs;
      if (Array.isArray(finishedRaw)) {
        for (const key of finishedRaw) {
          if (typeof key === "string" && key) {
            finished.add(String(key).trim().toUpperCase());
          }
        }
      } else if (finishedRaw && typeof finishedRaw === "object") {
        for (const key of Object.keys(finishedRaw)) {
          if (key) {
            finished.add(String(key).trim().toUpperCase());
          }
        }
      }
    } catch (_err) {
      // ignore malformed CompleteSave blocks
    }
  }

  return finished;
}

function humanizeObjectiveKey(key) {
  const raw = String(key || "").trim();
  if (!raw) {
    return "";
  }
  let name = raw;
  if (/^[A-Z]{2}_[0-9]{2}_[0-9]{2}_/.test(name)) {
    name = name.replace(/^[A-Z]{2}_[0-9]{2}_[0-9]{2}_/, "");
  } else if (/^[A-Z]{2}_[0-9]{2}_/.test(name)) {
    name = name.replace(/^[A-Z]{2}_[0-9]{2}_/, "");
  }
  name = name.replace(/_(OBJ|TSK|CNT|TASK|CONTRACT|CONTEST)$/i, "");
  name = name.replace(/__+/g, "_").replace(/^_+|_+$/g, "");
  const words = name.split("_").filter(Boolean);
  if (!words.length) {
    return raw;
  }
  return words
    .map((word) => {
      if (/^[0-9]+$/.test(word)) {
        return word;
      }
      if (/^[IVXLCDM]+$/.test(word)) {
        return word;
      }
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    })
    .join(" ");
}

function isObjectiveTokenLikeName(name, key) {
  const text = String(name || "").trim();
  if (!text) {
    return true;
  }
  const upper = text.toUpperCase();
  if (upper === String(key || "").toUpperCase()) {
    return true;
  }
  if (isObjectiveKey(upper)) {
    return true;
  }
  if (/^(UI_|EXP_)/.test(upper)) {
    return true;
  }
  return /^[A-Z0-9_]+$/.test(text) && text.includes("_");
}

function getObjectiveDisplayName(key, entry, meta) {
  const candidates = [
    entry && typeof entry === "object" ? entry.displayName || entry.name || entry.title || entry.label : "",
    state.objectives.catalogNames[key],
    meta && typeof meta === "object" ? meta.displayName || meta.name || meta.title : "",
  ];
  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (!text) {
      continue;
    }
    if (isObjectiveTokenLikeName(text, key)) {
      continue;
    }
    return text;
  }
  return humanizeObjectiveKey(key) || key;
}

function getObjectiveTypeForFilter(key, meta) {
  const source = String((meta && (meta.source || meta.category)) || "").trim().toUpperCase();
  if (source.includes("TASK")) {
    return "TASK";
  }
  if (source.includes("CONTRACT")) {
    return "CONTRACT";
  }
  if (source.includes("CONTEST")) {
    return "CONTEST";
  }

  const upperKey = String(key || "").toUpperCase();
  if (/_CNT\b|_CONTEST\b/.test(upperKey)) {
    return "CONTEST";
  }
  if (/_OBJ\b|_CONTRACT\b/.test(upperKey)) {
    return "CONTRACT";
  }
  if (/_TSK\b|_TASK\b/.test(upperKey)) {
    return "TASK";
  }
  return "";
}

function getObjectiveCategoryForFilter(meta) {
  const category = String((meta && meta.type) || "").trim().toLowerCase();
  if (category === "truckdelivery" || category === "truck_delivery") {
    return "truckDelivery";
  }
  if (category === "cargodelivery" || category === "cargo_delivery") {
    return "cargoDelivery";
  }
  if (category === "exploration" || category === "explore") {
    return "exploration";
  }
  return "";
}

function getObjectiveRegionForFilter(key, meta) {
  const regionName = cleanObjectiveMetaField(meta && (meta.regionName || meta.region || ""));
  if (regionName) {
    return regionName;
  }
  const match = /^([A-Z]{2}_[0-9]{2})/.exec(String(key || "").toUpperCase());
  return match ? match[1] : "";
}

function readObjectivesFilters() {
  return {
    search: String(els.objectivesSearch.value || "").trim().toLowerCase(),
    type: String(els.objectivesType.value || "").trim().toUpperCase(),
    region: String(els.objectivesRegion.value || "").trim(),
    category: String(els.objectivesCategory.value || "").trim(),
  };
}

function refreshObjectivesRegionFilter(keys) {
  if (!els.objectivesRegion) {
    return;
  }
  const selected = String(els.objectivesRegion.value || "");
  const regions = [];
  const seen = new Set();

  for (const key of keys) {
    const meta = state.objectives.catalogMeta[key];
    const region = getObjectiveRegionForFilter(key, meta);
    if (!region || seen.has(region)) {
      continue;
    }
    seen.add(region);
    regions.push(region);
  }
  regions.sort((a, b) => a.localeCompare(b));

  els.objectivesRegion.innerHTML = "";
  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "All regions";
  els.objectivesRegion.append(allOption);

  for (const region of regions) {
    const option = document.createElement("option");
    option.value = region;
    option.textContent = region;
    els.objectivesRegion.append(option);
  }

  if (selected && seen.has(selected)) {
    els.objectivesRegion.value = selected;
  } else {
    els.objectivesRegion.value = "";
  }
}

function isObjectiveInSearch(key, displayName, meta, search) {
  if (!search) {
    return true;
  }
  const parts = [
    key,
    displayName,
    meta && meta.category,
    meta && meta.region,
    meta && meta.regionName,
    meta && meta.type,
    meta && meta.cargoNeeded,
    meta && meta.descriptionText,
  ];
  return parts.some((part) => String(part || "").toLowerCase().includes(search));
}

function matchesObjectiveFilters(key, entry, filters) {
  const meta = state.objectives.catalogMeta[key];
  const displayName = getObjectiveDisplayName(key, entry, meta);
  if (!isObjectiveInSearch(key, displayName, meta, filters.search)) {
    return false;
  }
  if (filters.type && getObjectiveTypeForFilter(key, meta) !== filters.type) {
    return false;
  }
  if (filters.region && getObjectiveRegionForFilter(key, meta) !== filters.region) {
    return false;
  }
  if (filters.category && getObjectiveCategoryForFilter(meta) !== filters.category) {
    return false;
  }
  return true;
}

function buildObjectiveMetaParts(meta, includeRewards) {
  if (!meta || typeof meta !== "object") {
    return [];
  }
  const parts = [];
  if (meta.category) {
    parts.push(String(meta.category).replace(/^_+/, ""));
  }
  if (meta.regionName || meta.region) {
    parts.push(meta.regionName || meta.region);
  }
  if (meta.type) {
    parts.push(meta.type);
  }
  if (includeRewards && (meta.experience || meta.money)) {
    const rewards = [];
    if (meta.experience) {
      rewards.push(`${meta.experience} XP`);
    }
    if (meta.money) {
      rewards.push(`${meta.money} money`);
    }
    if (rewards.length) {
      parts.push(rewards.join(" / "));
    }
  }
  if (meta.cargoNeeded) {
    parts.push(meta.cargoNeeded);
  }
  return parts;
}

function isObjectiveFinishedEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return false;
  }
  return Boolean(entry.isFinished === true || entry.wasCompletedAtLeastOnce === true);
}

function renderObjectivesList() {
  els.objectivesList.innerHTML = "";

  if (!state.objectives.data) {
    if (!state.main && state.objectives.catalogKeys.size > 0) {
      const keys = [...state.objectives.catalogKeys].sort();
      refreshObjectivesRegionFilter(keys);
      const filters = readObjectivesFilters();
      const visible = keys.filter((key) => matchesObjectiveFilters(key, null, filters));
      state.objectives.visibleKeys = visible;

      for (const key of visible) {
        const meta = state.objectives.catalogMeta[key];
        const displayName = getObjectiveDisplayName(key, null, meta) || humanizeObjectiveKey(key) || key;
        const row = document.createElement("div");
        row.className = "objective-row objective-row-no-status";

        const infoWrap = document.createElement("div");
        const nameText = document.createElement("div");
        nameText.className = "objective-title";
        nameText.textContent = displayName;
        infoWrap.append(nameText);

        const metaParts = buildObjectiveMetaParts(meta, false);
        if (metaParts.length) {
          const metaText = document.createElement("div");
          metaText.className = "help";
          metaText.textContent = metaParts.join(" | ");
          infoWrap.append(metaText);
        }

        const check = document.createElement("input");
        check.type = "checkbox";
        check.checked = state.objectives.selected.has(key);
        check.addEventListener("change", () => {
          if (check.checked) {
            state.objectives.selected.add(key);
          } else {
            state.objectives.selected.delete(key);
          }
          updateObjectivesInfo({
            mode: "catalog",
            total: keys.length,
            filtered: visible.length,
            shown: visible.length,
          });
        });

        row.append(infoWrap, check);
        els.objectivesList.append(row);
      }

      updateObjectivesInfo({
        mode: "catalog",
        total: keys.length,
        filtered: visible.length,
        shown: visible.length,
      });
      return;
    }

    if (state.main) {
      els.objectivesInfo.textContent = "objectiveStates not loaded.";
    } else {
      setObjectivesNeedMainMessage();
    }
    return;
  }

  const keys = state.objectives.catalogKeys.size
    ? [...state.objectives.catalogKeys].sort()
    : Object.keys(state.objectives.data).sort();
  const regionKeys = state.objectives.catalogKeys.size ? [...state.objectives.catalogKeys] : keys;
  refreshObjectivesRegionFilter(regionKeys);
  const filters = readObjectivesFilters();
  const visible = keys.filter((key) => matchesObjectiveFilters(key, state.objectives.data[key], filters));
  state.objectives.visibleKeys = visible;

  let finishedCount = 0;
  for (const key of visible) {
    const entry = state.objectives.data[key];
    const isFinished =
      state.objectives.finishedInSave.has(String(key || "").toUpperCase()) ||
      isObjectiveFinishedEntry(entry);
    if (isFinished) {
      finishedCount += 1;
    }

    const meta = state.objectives.catalogMeta[key];
    const displayName = getObjectiveDisplayName(key, entry, meta) || humanizeObjectiveKey(key) || key;
    const row = document.createElement("div");
    row.className = "objective-row";

    const infoWrap = document.createElement("div");
    const nameText = document.createElement("div");
    nameText.className = "objective-title";
    nameText.textContent = displayName;
    infoWrap.append(nameText);

    const metaParts = buildObjectiveMetaParts(meta, true);
    if (metaParts.length) {
      const metaText = document.createElement("div");
      metaText.className = "help";
      metaText.textContent = metaParts.join(" | ");
      infoWrap.append(metaText);
    }

    const status = document.createElement("span");
    status.className = "pill";
    status.textContent = isFinished ? "finished" : "pending";

    const check = document.createElement("input");
    check.type = "checkbox";
    if (isFinished) {
      state.objectives.selected.delete(key);
    }
    check.checked = isFinished || state.objectives.selected.has(key);
    check.disabled = isFinished;
    check.addEventListener("change", () => {
      if (isFinished) {
        return;
      }
      if (check.checked) {
        state.objectives.selected.add(key);
      } else {
        state.objectives.selected.delete(key);
      }
      updateObjectivesInfo({
        mode: "main",
        total: keys.length,
        filtered: visible.length,
        shown: visible.length,
        finishedShown: finishedCount,
      });
    });

    row.append(infoWrap, status, check);
    els.objectivesList.append(row);
  }

  updateObjectivesInfo({
    mode: "main",
    total: keys.length,
    filtered: visible.length,
    shown: visible.length,
    finishedShown: finishedCount,
  });
}

function updateObjectivesInfo(summary) {
  const source = state.objectives.catalogSource ? ` | Source: ${state.objectives.catalogSource}` : "";
  if (summary.mode === "catalog") {
    els.objectivesInfo.textContent =
      `Catalog entries: ${summary.total} | Filtered: ${summary.filtered} | Showing: ${summary.shown} | Selected: ${state.objectives.selected.size} | Upload Main Save to apply changes.${source}`;
    return;
  }
  els.objectivesInfo.textContent =
    `Total: ${summary.total} | Filtered: ${summary.filtered} | Showing: ${summary.shown} | Finished shown: ${summary.finishedShown || 0} | Selected: ${state.objectives.selected.size}${source}`;
}

function setObjectivesNeedMainMessage() {
  const catalogLoaded = Math.max(Object.keys(state.objectives.catalogNames).length, state.objectives.catalogKeys.size);
  const source = state.objectives.catalogSource || "embedded";
  if (catalogLoaded > 0) {
    els.objectivesInfo.textContent = `Catalog ready (${catalogLoaded} entries, source: ${source}). Upload Main Save to load objectiveStates.`;
  } else {
    els.objectivesInfo.textContent = "Upload Main Save to load objectiveStates.";
  }
}

function selectVisibleObjectives() {
  for (const key of state.objectives.visibleKeys) {
    if (state.objectives.finishedInSave.has(String(key || "").toUpperCase())) {
      continue;
    }
    if (state.objectives.data && isObjectiveFinishedEntry(state.objectives.data[key])) {
      continue;
    }
    state.objectives.selected.add(key);
  }
  renderObjectivesList();
}

function clearObjectiveSelection() {
  state.objectives.selected.clear();
  renderObjectivesList();
}

function scheduleObjectivesCatalogWarmup() {
  if (state.objectives.catalogLoaded || state.objectives.catalogLoading || state.objectives.catalogWarmupScheduled) {
    return;
  }
  state.objectives.catalogWarmupScheduled = true;
  const run = () => {
    state.objectives.catalogWarmupScheduled = false;
    ensureObjectivesCatalogLoaded(true);
  };
  if (typeof window !== "undefined" && typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(run, { timeout: 2000 });
  } else {
    window.setTimeout(run, 550);
  }
}

function ensureObjectivesCatalogLoaded(silent = false) {
  if (state.objectives.catalogLoaded) {
    return true;
  }
  if (!state.objectives.catalogLoadPromise) {
    state.objectives.catalogLoadPromise = loadEmbeddedObjectivesCatalog(silent)
      .catch((error) => {
        if (!silent) {
          setStatus(`Objectives+ catalog failed to load: ${error.message}`, "error");
        }
      })
      .finally(() => {
        state.objectives.catalogLoadPromise = null;
      });
  }
  return false;
}

async function loadObjectivesCatalogFromScript() {
  const existing = typeof window !== "undefined" ? window.__OBJECTIVES_CATALOG_CSV__ : "";
  if (typeof existing === "string" && existing.trim()) {
    return existing;
  }
  if (!objectivesCatalogScriptPromise) {
    objectivesCatalogScriptPromise = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = OBJECTIVES_CATALOG_SCRIPT_URL;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => reject(new Error("catalog script failed"));
      document.head.append(script);
    }).finally(() => {
      objectivesCatalogScriptPromise = null;
    });
  }
  await objectivesCatalogScriptPromise;
  const loaded = typeof window !== "undefined" ? window.__OBJECTIVES_CATALOG_CSV__ : "";
  return typeof loaded === "string" ? loaded : "";
}

async function loadEmbeddedObjectivesCatalog(silent = false) {
  if (state.objectives.catalogLoaded || state.objectives.catalogLoading) {
    return;
  }
  state.objectives.catalogLoading = true;

  let raw = "";
  let sourceLabel = "embedded";

  try {
    raw = await loadObjectivesCatalogFromScript();
    if (raw) {
      sourceLabel = "embedded file";
    }
  } catch (_scriptError) {
    // Fall back to CSV fetch and final inline fallback.
  }

  try {
    if (!raw && OBJECTIVES_CATALOG_CSV_URL && typeof fetch === "function") {
      const response = await fetch(OBJECTIVES_CATALOG_CSV_URL, { cache: "force-cache" });
      if (response.ok) {
        raw = await response.text();
        sourceLabel = "local file";
      }
    }
  } catch (_error) {
    // Fall back to inline data if local catalog fetch fails.
  }

  if (!raw) {
    raw = String(EMBEDDED_OBJECTIVES_CSV || "");
    sourceLabel = "embedded";
  }

  if (!raw) {
    state.objectives.catalogLoading = false;
    if (!silent) {
      setStatus("Objectives+ catalog is missing.", "error");
    }
    return;
  }

  try {
    const count = applyObjectiveCatalog(raw, sourceLabel);
    state.objectives.catalogLoaded = count > 0;
    if (!silent) {
      setStatus(`Objectives+ catalog loaded (${count} keys).`, "success");
    }
    if (document.querySelector(".tab-btn.active")?.dataset?.tab === "objectives") {
      renderObjectivesList();
    }
  } catch (error) {
    if (!silent) {
      setStatus(`Objectives+ catalog failed to load: ${error.message}`, "error");
    }
  } finally {
    state.objectives.catalogLoading = false;
  }
}

function applyObjectiveCatalog(text, sourceLabel) {
  const parsed = parseOnlineObjectiveCatalog(text);
  if (!parsed.keys.size) {
    throw new Error("No objective keys found in embedded catalog.");
  }
  state.objectives.catalogNames = parsed.names;
  state.objectives.catalogKeys = parsed.keys;
  state.objectives.catalogMeta = parsed.meta;
  state.objectives.catalogSource = sourceLabel || "";
  return parsed.keys.size;
}

function parseOnlineObjectiveCatalog(text) {
  const trimmed = String(text || "").trim();
  const names = {};
  const keys = new Set();
  const meta = {};

  const csvParsed = parseObjectiveCsvCatalog(trimmed);
  if (csvParsed) {
    return csvParsed;
  }

  // If source is JSON, support object map or list rows.
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      const data = JSON.parse(trimmed);
      if (Array.isArray(data)) {
        for (const row of data) {
          addObjectiveCatalogEntry(keys, names, meta, row, "");
        }
      } else if (data && typeof data === "object") {
        for (const [k, v] of Object.entries(data)) {
          addObjectiveCatalogEntry(keys, names, meta, v, k);
        }
      }
      if (keys.size > 0) {
        return { keys, names, meta };
      }
    } catch (_err) {
      // fall through to regex mode
    }
  }

  // Generic text fallback: extract objective-like keys.
  const pattern = /\b[A-Z]{2}_[0-9]{2}_[A-Z0-9_]{3,}\b/g;
  const all = trimmed.match(pattern) || [];
  for (const token of all) {
    const key = String(token || "").toUpperCase();
    if (isLikelyObjectiveKey(key)) {
      keys.add(key);
    }
  }

  return { keys, names, meta };
}

function parseObjectiveCsvCatalog(text) {
  const rows = parseCsvRows(text);
  if (!rows.length) {
    return null;
  }
  const header = rows[0].map((cell) => String(cell || "").replace(/^\uFEFF/, "").trim().toLowerCase());
  const keyIndex = header.findIndex((name) => name === "key" || name === "objective" || name === "id");
  if (keyIndex === -1) {
    return null;
  }

  const keys = new Set();
  const names = {};
  const meta = {};
  for (let i = 1; i < rows.length; i += 1) {
    const record = {};
    for (let j = 0; j < header.length; j += 1) {
      record[header[j]] = rows[i][j] == null ? "" : rows[i][j];
    }
    addObjectiveCatalogEntry(keys, names, meta, record, record.key || record.objective || record.id || "");
  }

  if (!keys.size) {
    return null;
  }
  return { keys, names, meta };
}

function parseCsvRows(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === "\"") {
        if (text[i + 1] === "\"") {
          cell += "\"";
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += ch;
      }
      continue;
    }
    if (ch === "\"") {
      inQuotes = true;
      continue;
    }
    if (ch === ",") {
      row.push(cell);
      cell = "";
      continue;
    }
    if (ch === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }
    if (ch === "\r") {
      continue;
    }
    cell += ch;
  }
  if (cell.length || row.length) {
    row.push(cell);
    rows.push(row);
  }
  return rows;
}

function addObjectiveCatalogEntry(keys, names, meta, payload, keyHint) {
  const key =
    typeof keyHint === "string" && keyHint
      ? String(keyHint).trim().toUpperCase()
      : String(payload && (payload.key || payload.objective || payload.id || "")).trim().toUpperCase();
  if (!isObjectiveKey(key)) {
    return;
  }

  keys.add(key);

  if (typeof payload === "string") {
    const name = payload.trim();
    if (name) {
      names[key] = name;
    }
    return;
  }

  if (!payload || typeof payload !== "object") {
    return;
  }

  const name = String(payload.displayName || payload.displayname || payload.name || payload.title || payload.label || "").trim();
  if (name) {
    names[key] = name;
  }

  const entryMeta = {
    category: cleanObjectiveMetaField(payload.category || payload.group || ""),
    region: cleanObjectiveMetaField(payload.region || payload.region_code || payload.regioncode || ""),
    regionName: cleanObjectiveMetaField(payload.region_name || payload.regionname || ""),
    type: cleanObjectiveMetaField(payload.type || ""),
    cargoNeeded: cleanObjectiveMetaField(payload.cargo_needed || payload.cargoneeded || ""),
    experience: cleanObjectiveMetaField(payload.experience || payload.xp || ""),
    money: cleanObjectiveMetaField(payload.money || payload.reward_money || ""),
    descriptionText: cleanObjectiveMetaField(payload.descriptionText || payload.descriptiontext || payload.description || ""),
    source: cleanObjectiveMetaField(payload.source || payload.Source || ""),
  };

  if (Object.values(entryMeta).some((value) => value)) {
    meta[key] = entryMeta;
  }
}

function cleanObjectiveMetaField(value) {
  if (value == null) {
    return "";
  }
  return String(value).replace(/\s+/g, " ").trim();
}

function isObjectiveKey(token) {
  if (!token) {
    return false;
  }
  const s = String(token).toUpperCase();
  return /^[A-Z]{2}_[0-9]{2}_[A-Z0-9_]{3,}$/.test(s);
}

function isLikelyObjectiveKey(token) {
  if (!isObjectiveKey(token)) {
    return false;
  }
  return (
    token.includes("_OBJ") ||
    token.includes("_TSK") ||
    token.includes("_CNT") ||
    token.includes("_TASK") ||
    token.includes("_CONTRACT") ||
    token.includes("_CONTEST") ||
    token.includes("_DELIVER") ||
    token.includes("_DELIVERY")
  );
}

function completeSelectedObjectives() {
  if (!requireMain()) {
    return;
  }
  if (state.objectives.selected.size === 0) {
    setStatus("No objectives selected.", "error");
    return;
  }
  try {
    const m = /"objectiveStates"\s*:\s*\{/i.exec(state.main.text);
    if (!m) {
      throw new Error('"objectiveStates" block not found.');
    }
    const block = extractBraceBlock(state.main.text, m.index);
    const obj = JSON.parse(block.block);
    let updated = 0;
    for (const key of state.objectives.selected) {
      let entry = obj[key];
      if (!entry || typeof entry !== "object") {
        entry = {};
        obj[key] = entry;
      }
      if (entry.isFinished !== true || entry.wasCompletedAtLeastOnce !== true) {
        entry.isFinished = true;
        entry.wasCompletedAtLeastOnce = true;
        updated += 1;
      }
    }
    const newBlock = JSON.stringify(obj);
    const content = state.main.text.slice(0, block.start) + newBlock + state.main.text.slice(block.end);
    state.objectives.selected.clear();
    commitMain(content, `Objectives+ updated (${updated} completed).`);
  } catch (error) {
    setStatus(`Failed to complete objectives: ${error.message}`, "error");
  }
}

function onApplyMoneyRank() {
  if (!requireMain()) {
    return;
  }
  const moneyInput = parseOptionalMoneyInt(els.moneyInput.value);
  const rankInput = parseOptionalStrictInt(els.rankInput.value);
  const xpInput = parseOptionalStrictInt(els.xpInput.value);

  if (moneyInput.error) {
    setStatus("Money must be an integer (e.g. -100 or 12345).", "error");
    return;
  }
  if (rankInput.error) {
    setStatus("Rank must be a whole number in range 1-30.", "error");
    return;
  }
  if (xpInput.error) {
    setStatus("Experience must be a non-negative whole number.", "error");
    return;
  }

  const money = moneyInput.value;
  let rank = rankInput.value;
  let xp = xpInput.value;

  if (money == null && rank == null && xp == null) {
    setStatus("Enter at least one value.", "error");
    return;
  }
  if (rank != null && (rank < 1 || rank > 30)) {
    setStatus("Rank must be in range 1-30.", "error");
    return;
  }
  if (xp != null && xp < 0) {
    setStatus("Experience must be non-negative.", "error");
    return;
  }

  // Keep rank/experience linked like the desktop app:
  // rank change sets XP, XP change computes rank.
  if (rank != null && xp == null) {
    xp = xpForRank(rank);
  } else if (xp != null && rank == null) {
    rank = rankForXp(xp);
  } else if (rank != null && xp != null) {
    xp = xpForRank(rank);
  }

  try {
    let content = state.main.text;
    const writeMeta = [];
    if (money != null) {
      const out = replaceOrInsertNumericWithCount(content, "money", money);
      content = out.content;
      writeMeta.push({ key: "money", count: out.count });
    }
    if (rank != null) {
      const out = replaceOrInsertNumericWithCount(content, "rank", rank);
      content = out.content;
      writeMeta.push({ key: "rank", count: out.count });
    }
    if (xp != null) {
      const out = replaceOrInsertNumericWithCount(content, "experience", xp);
      content = out.content;
      writeMeta.push({ key: "experience", count: out.count });
    }
    let message = "Money/Rank/XP updated.";
    if (moneyInput.clamped) {
      message += ` Money clamped to ${money} (allowed ${MONEY_MIN} to ${MONEY_MAX}).`;
    }
    const duplicates = writeMeta.filter((entry) => entry.count > 1);
    if (duplicates.length > 0) {
      const details = duplicates.map((entry) => `${entry.key} x${entry.count}`).join(", ");
      message += ` Duplicate keys updated: ${details}.`;
    }
    commitMain(content, message);
  } catch (error) {
    setStatus(`Failed to update money/rank/xp: ${error.message}`, "error");
  }
}

function onApplyTime() {
  if (!requireMain()) {
    return;
  }
  const dayInput = parseOptionalStrictFloat(els.timeDayInput.value);
  const nightInput = parseOptionalStrictFloat(els.timeNightInput.value);
  if (dayInput.error) {
    setStatus("Day speed must be a valid number.", "error");
    return;
  }
  if (nightInput.error) {
    setStatus("Night speed must be a valid number.", "error");
    return;
  }
  let day = dayInput.value;
  let night = nightInput.value;
  const skip = Boolean(els.skipTimeInput.checked);

  const current = getFileInfo(state.main.text);
  if (day == null) {
    day = current.day;
  }
  if (night == null) {
    night = current.night;
  }
  if (day == null || night == null) {
    setStatus("Set both day and night values.", "error");
    return;
  }

  try {
    let content = state.main.text;
    content = replaceOrInsertNumeric(content, "timeSettingsDay", day);
    content = replaceOrInsertNumeric(content, "timeSettingsNight", night);
    content = replaceOrInsertBoolean(content, "isAbleToSkipTime", skip);
    commitMain(content, "Time settings updated.");
  } catch (error) {
    setStatus(`Failed to update time: ${error.message}`, "error");
  }
}

function onApplyMissions() {
  if (!requireMain()) {
    return;
  }
  const selectedSeasons = state.selectors.missions.getSelectedSeasons();
  const selectedMaps = state.selectors.missions.getSelectedMaps();
  if (selectedSeasons.length === 0 && selectedMaps.length === 0) {
    setStatus("Select at least one season or map.", "error");
    return;
  }
  try {
    const out = completeSeasonsAndMaps(state.main.text, selectedSeasons, selectedMaps);
    commitMain(out.content, out.message);
  } catch (error) {
    setStatus(`Failed to complete missions: ${error.message}`, "error");
  }
}

function onApplyContests() {
  if (!requireMain()) {
    return;
  }
  const selectedSeasons = state.selectors.contests.getSelectedSeasons();
  const selectedMaps = state.selectors.contests.getSelectedMaps();
  if (selectedSeasons.length === 0 && selectedMaps.length === 0) {
    setStatus("Select at least one season or map.", "error");
    return;
  }
  try {
    const out = markDiscoveredContestsComplete(state.main.text, selectedSeasons, selectedMaps);
    commitMain(out.content, out.message);
  } catch (error) {
    setStatus(`Failed to complete contests: ${error.message}`, "error");
  }
}

function onApplyRegions() {
  if (!requireMain()) {
    return;
  }
  const selector = state.selectors.regions;
  if (!selector) {
    setStatus("Regions+ selector unavailable.", "error");
    return;
  }

  const selectedFeatures = getSelectedRegionsFeatures();
  if (selectedFeatures.length === 0) {
    setStatus("Select at least one Regions+ feature.", "error");
    return;
  }

  const selectedSeasons = selector.getSelectedSeasons();
  const selectedMaps = selector.getSelectedMaps();
  const selectedRegions = selector.getSelectedRegions();
  if (selectedSeasons.length === 0 && selectedMaps.length === 0 && selectedRegions.length === 0) {
    setStatus("Select at least one season or map.", "error");
    return;
  }

  try {
    let content = state.main.text;
    const messages = [];
    for (const item of selectedFeatures) {
      const def = item.definition;
      let out = null;
      if (def.mode === "seasonMap") {
        out =
          def.key === "missions"
            ? completeSeasonsAndMaps(content, selectedSeasons, selectedMaps)
            : markDiscoveredContestsComplete(content, selectedSeasons, selectedMaps);
      } else if (def.key === "upgrades") {
        out = unlockUpgrades(content, selectedRegions);
      } else if (def.key === "watchtowers") {
        out = unlockWatchtowers(content, selectedRegions);
      } else if (def.key === "discoveries") {
        out = unlockDiscoveries(content, selectedRegions);
      } else if (def.key === "levels") {
        out = unlockLevels(content, selectedRegions);
      } else if (def.key === "garages") {
        out = unlockGarages(content, selectedRegions, item.extra);
      }
      if (!out || typeof out.content !== "string") {
        throw new Error(`Regions+ action failed for ${def.label}.`);
      }
      content = out.content;
      messages.push(`${def.label}: ${out.message || "applied"}`);
    }

    if (els.regionsInfo) {
      els.regionsInfo.textContent = messages.join(" ");
    }
    commitMain(
      content,
      `Regions+ updated (${selectedFeatures.length} feature${selectedFeatures.length === 1 ? "" : "s"}).`,
    );
  } catch (error) {
    setStatus(`Failed Regions+ action: ${error.message}`, "error");
  }
}

function onApplyRegionWithSelector(selector, mutator, successMessage) {
  if (!requireMain()) {
    return;
  }
  if (!selector) {
    setStatus("Region selector unavailable.", "error");
    return;
  }
  const selectedRegions = selector.getSelectedRegions();
  if (selectedRegions.length === 0) {
    setStatus("Select at least one region.", "error");
    return;
  }
  try {
    const out = mutator(state.main.text, selectedRegions);
    commitMain(out.content, out.message || successMessage);
  } catch (error) {
    setStatus(`Failed region action: ${error.message}`, "error");
  }
}

function onSaveTrials() {
  if (!requireCommon()) {
    return;
  }
  try {
    const selected = [];
    for (const [code, input] of state.trialChecks.entries()) {
      if (input.checked) {
        selected.push(code);
      }
    }
    const newText = writeFinishedTrials(state.common.text, selected);
    commitCommon(newText, "Trials updated.");
  } catch (error) {
    setStatus(`Failed to save trials: ${error.message}`, "error");
  }
}

function onSavePros() {
  if (!requireCommon()) {
    return;
  }
  try {
    const container = parseCommonContainer(state.common.text);
    const ssl = getCommonSslValue(container.parsed);
    let ent = ssl.givenProsEntitlements;
    if (!Array.isArray(ent)) {
      ent = [];
    }
    for (const [key] of PROS_ENTITLEMENTS) {
      const checked = key === "ProsRegistrationReward" ? els.prosRegistration.checked : els.prosRoadcraft.checked;
      if (checked && !ent.includes(key)) {
        ent.push(key);
      }
      if (!checked) {
        ent = ent.filter((v) => v !== key);
      }
    }
    ssl.givenProsEntitlements = ent;
    const newText = writeCommonContainer(state.common.text, container);
    commitCommon(newText, "PROS entitlements updated.");
  } catch (error) {
    setStatus(`Failed to save PROS: ${error.message}`, "error");
  }
}

function onUnlockAchievements() {
  if (!requireCommon()) {
    return;
  }
  try {
    const container = parseCommonContainer(state.common.text);
    const ssl = getCommonSslValue(container.parsed);
    const states = ssl.achievementStates;
    if (!states || typeof states !== "object" || Array.isArray(states)) {
      throw new Error("achievementStates not found.");
    }
    let updated = 0;
    for (const [key, value] of Object.entries(states)) {
      if (value && typeof value === "object" && !Array.isArray(value)) {
        if (value.isUnlocked !== true) {
          value.isUnlocked = true;
          updated += 1;
        }
      } else {
        states[key] = { isUnlocked: true };
        updated += 1;
      }
    }
    const newText = writeCommonContainer(state.common.text, container);
    commitCommon(newText, `Achievements unlocked for ${updated} entries.`);
  } catch (error) {
    setStatus(`Failed to unlock achievements: ${error.message}`, "error");
  }
}

function refreshTrialsFromCommon() {
  if (!state.common) {
    setAllTrials(false);
    return;
  }
  const finished = readFinishedTrials(state.common.text);
  for (const [code, input] of state.trialChecks.entries()) {
    input.checked = finished.includes(code);
  }
}

function refreshProsFromCommon() {
  els.prosRegistration.checked = false;
  els.prosRoadcraft.checked = false;
  if (!state.common) {
    return;
  }
  try {
    const container = parseCommonContainer(state.common.text);
    const ssl = getCommonSslValue(container.parsed);
    const ent = Array.isArray(ssl.givenProsEntitlements) ? ssl.givenProsEntitlements : [];
    els.prosRegistration.checked = ent.includes("ProsRegistrationReward");
    els.prosRoadcraft.checked = ent.includes("ProsRoadcraftReward");
  } catch (_err) {
    // keep controls clear on parse error
  }
}

function refreshAchievementsFromCommon() {
  if (!state.common) {
    els.achievementsStats.textContent = "No CommonSslSave loaded.";
    return;
  }
  try {
    const container = parseCommonContainer(state.common.text);
    const ssl = getCommonSslValue(container.parsed);
    const states = ssl.achievementStates;
    if (!states || typeof states !== "object" || Array.isArray(states)) {
      els.achievementsStats.textContent = "No achievementStates block found in CommonSslSave.";
      return;
    }
    const total = Object.keys(states).length;
    let unlocked = 0;
    for (const value of Object.values(states)) {
      if (value && typeof value === "object" && value.isUnlocked) {
        unlocked += 1;
      }
    }
    els.achievementsStats.textContent = `Achievements found: ${total}. Unlocked: ${unlocked}.`;
  } catch (error) {
    els.achievementsStats.textContent = `CommonSslSave parse issue: ${error.message}`;
  }
}

function getFileInfo(content) {
  return {
    money: readMaxIntKey(content, "money"),
    rank: readMaxIntKey(content, "rank"),
    xp: readMaxIntKey(content, "experience"),
    day: readNumericKey(content, "timeSettingsDay"),
    night: readNumericKey(content, "timeSettingsNight"),
    skipTime: readBoolKey(content, "isAbleToSkipTime"),
  };
}

function completeSeasonsAndMaps(content, selectedSeasons, selectedMaps) {
  const m = /"objectiveStates"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"objectiveStates" block not found.');
  }
  const block = extractBraceBlock(content, m.index);
  const objStates = JSON.parse(block.block);
  let modified = 0;
  for (const key of Object.keys(objStates)) {
    const entry = objStates[key];
    if (!entry || typeof entry !== "object") {
      continue;
    }
    let matched = false;
    for (const season of selectedSeasons) {
      const mapCode = SEASON_ID_MAP[season];
      if ((mapCode && key.includes(mapCode)) || key.includes(`_${String(season).padStart(2, "0")}_`)) {
        matched = true;
        break;
      }
    }
    if (!matched) {
      for (const map of selectedMaps) {
        if (key.includes(map)) {
          matched = true;
          break;
        }
      }
    }
    if (!matched) {
      continue;
    }
    if (entry.isFinished !== true || entry.wasCompletedAtLeastOnce !== true) {
      entry.isFinished = true;
      entry.wasCompletedAtLeastOnce = true;
      modified += 1;
    }
  }
  const newBlock = JSON.stringify(objStates);
  const nextContent = content.slice(0, block.start) + newBlock + content.slice(block.end);
  if (modified === 0) {
    return { content: nextContent, message: "No matching missions found." };
  }
  return { content: nextContent, message: `Selected missions marked complete (${modified} updated).` };
}

function markDiscoveredContestsComplete(content, selectedSeasons, selectedMaps) {
  const selectedRegionCodes = selectedSeasons.map((s) => SEASON_ID_MAP[s]).filter(Boolean);
  const seasonTokens = selectedSeasons.map((s) => `_${String(s).padStart(2, "0")}_`);
  const matches = [];
  const re = /"(CompleteSave\d*)"\s*:\s*\{/g;
  let m = null;
  while ((m = re.exec(content)) !== null) {
    matches.push({ key: m[1], index: m.index });
  }

  let changedBlocks = 0;
  let totalAdded = 0;
  const globalContestTimesNewEntries = {};
  for (let i = matches.length - 1; i >= 0; i -= 1) {
    const hit = matches[i];
    const valueBlock = extractBraceBlock(content, hit.index);
    let valueData = null;
    try {
      valueData = JSON.parse(valueBlock.block);
    } catch (_err) {
      continue;
    }

    let ssl = valueData.SslValue;
    if (!ssl || typeof ssl !== "object") {
      const maybeNested = valueData[hit.key];
      ssl = maybeNested && typeof maybeNested === "object" ? maybeNested.SslValue : null;
    }
    if (!ssl || typeof ssl !== "object") {
      ssl = {};
    }

    const discoveredRaw = ssl.discoveredObjectives;
    let discovered = [];
    if (Array.isArray(discoveredRaw)) {
      discovered = discoveredRaw;
    } else if (discoveredRaw && typeof discoveredRaw === "object") {
      discovered = Object.keys(discoveredRaw);
    }

    const finishedRaw = ssl.finishedObjs;
    const finishedIsDict = Boolean(finishedRaw && typeof finishedRaw === "object" && !Array.isArray(finishedRaw));
    const finishedSet = new Set(
      Array.isArray(finishedRaw)
        ? finishedRaw
        : finishedIsDict
          ? Object.keys(finishedRaw)
          : [],
    );

    let contestTimes = ssl.contestTimes;
    if (!contestTimes || typeof contestTimes !== "object" || Array.isArray(contestTimes)) {
      contestTimes = {};
    }

    const addedKeys = [];
    for (const key of discovered) {
      if (typeof key !== "string") {
        continue;
      }
      let matched = seasonTokens.some((token) => key.includes(token));
      if (!matched) {
        for (const code of [...selectedRegionCodes, ...selectedMaps]) {
          if (code && key.includes(code)) {
            matched = true;
            break;
          }
        }
      }
      if (!matched) {
        continue;
      }
      if (!finishedSet.has(key)) {
        finishedSet.add(key);
        addedKeys.push(key);
      }
      if (!(key in contestTimes)) {
        contestTimes[key] = 1;
        globalContestTimesNewEntries[key] = 1;
      }
    }

    if (addedKeys.length === 0) {
      continue;
    }

    ssl.finishedObjs = finishedIsDict
      ? Object.fromEntries([...finishedSet].map((key) => [key, true]))
      : [...finishedSet];
    ssl.contestTimes = contestTimes;
    if (Array.isArray(ssl.viewedUnactivatedObjectives)) {
      ssl.viewedUnactivatedObjectives = ssl.viewedUnactivatedObjectives.filter((v) => !addedKeys.includes(v));
    }

    valueData.SslValue = ssl;
    const newBlock = JSON.stringify(valueData);
    content = content.slice(0, valueBlock.start) + newBlock + content.slice(valueBlock.end);
    changedBlocks += 1;
    totalAdded += addedKeys.length;
  }

  if (Object.keys(globalContestTimesNewEntries).length > 0) {
    content = updateAllContestTimesBlocks(content, globalContestTimesNewEntries);
  }

  if (changedBlocks === 0) {
    return { content, message: "No discovered contest entries matched selected regions." };
  }
  return {
    content,
    message: `Updated ${changedBlocks} CompleteSave block(s). Added ${totalAdded} finished entries.`,
  };
}

function ensureWatchpointsDefaults(wpData) {
  let added = 0;
  let data = wpData.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    data = {};
    wpData.data = data;
  }
  for (const [mapKey, towers] of Object.entries(SAFE_DEFAULTS.watchpointsUnlocks)) {
    let existing = data[mapKey];
    if (!existing || typeof existing !== "object" || Array.isArray(existing)) {
      existing = {};
      data[mapKey] = existing;
    }
    for (const towerKey of Object.keys(towers || {})) {
      if (!Object.prototype.hasOwnProperty.call(existing, towerKey)) {
        existing[towerKey] = false;
        added += 1;
      }
    }
  }
  return added;
}

function ensureUpgradesDefaults(upgradesData) {
  let added = 0;
  for (const [mapKey, upgrades] of Object.entries(SAFE_DEFAULTS.upgradesGiverUnlocks)) {
    let existing = upgradesData[mapKey];
    if (!existing || typeof existing !== "object" || Array.isArray(existing)) {
      existing = {};
      upgradesData[mapKey] = existing;
    }
    for (const upgradeKey of Object.keys(upgrades || {})) {
      if (!Object.prototype.hasOwnProperty.call(existing, upgradeKey)) {
        existing[upgradeKey] = 0;
        added += 1;
      }
    }
  }
  return added;
}

function ensureDiscoveredTrucksDefaults(dtData) {
  let added = 0;
  let out = dtData;
  if (!out || typeof out !== "object" || Array.isArray(out)) {
    out = {};
  }
  for (const [mapKey, values] of Object.entries(SAFE_DEFAULTS.discoveredTrucksDefaults)) {
    let entry = out[mapKey];
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      out[mapKey] = {
        current: Number.parseInt(String(values && values.current != null ? values.current : 0), 10) || 0,
        all: Number.parseInt(String(values && values.all != null ? values.all : 0), 10) || 0,
      };
      added += 1;
      continue;
    }
    if (!Object.prototype.hasOwnProperty.call(entry, "current")) {
      entry.current = Number.parseInt(String(values && values.current != null ? values.current : 0), 10) || 0;
      added += 1;
    }
    if (!Object.prototype.hasOwnProperty.call(entry, "all")) {
      entry.all = Number.parseInt(String(values && values.all != null ? values.all : 0), 10) || 0;
      added += 1;
    }
  }
  return { added, data: out };
}

function unlockWatchtowers(content, selectedRegions) {
  const m = /"watchPointsData"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"watchPointsData" not found.');
  }
  const block = extractBraceBlock(content, m.index);
  const wpData = JSON.parse(block.block);
  const addedDefaults = ensureWatchpointsDefaults(wpData);
  if (!wpData.data || typeof wpData.data !== "object" || Array.isArray(wpData.data)) {
    wpData.data = {};
  }

  let updated = 0;
  for (const [mapKey, towers] of Object.entries(wpData.data)) {
    if (!towers || typeof towers !== "object" || Array.isArray(towers)) {
      continue;
    }
    if (!selectedRegions.some((code) => mapKey.toLowerCase().includes(`level_${code.toLowerCase()}`))) {
      continue;
    }
    for (const towerKey of Object.keys(towers)) {
      if (towers[towerKey] === false) {
        towers[towerKey] = true;
        updated += 1;
      }
    }
  }

  const newBlock = JSON.stringify(wpData);
  const next = content.slice(0, block.start) + newBlock + content.slice(block.end);
  let message = `Unlocked ${updated} watchtower entries.`;
  if (addedDefaults > 0) {
    message += ` Added ${addedDefaults} missing entries.`;
  }
  return { content: next, message };
}

function unlockUpgrades(content, selectedRegions) {
  const m = /"upgradesGiverData"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"upgradesGiverData" not found.');
  }
  const block = extractBraceBlock(content, m.index);
  const data = JSON.parse(block.block);
  const addedDefaults = ensureUpgradesDefaults(data);
  let updated = 0;
  for (const [mapKey, upgrades] of Object.entries(data)) {
    if (!upgrades || typeof upgrades !== "object" || Array.isArray(upgrades)) {
      continue;
    }
    if (!selectedRegions.some((code) => mapKey.toLowerCase().includes(`level_${code.toLowerCase()}`))) {
      continue;
    }
    for (const key of Object.keys(upgrades)) {
      if (upgrades[key] === 0 || upgrades[key] === 1) {
        upgrades[key] = 2;
        updated += 1;
      }
    }
  }

  const newBlock = JSON.stringify(data);
  const next = content.slice(0, block.start) + newBlock + content.slice(block.end);
  let message = `Updated ${updated} upgrades.`;
  if (addedDefaults > 0) {
    message += ` Added ${addedDefaults} missing entries.`;
  }
  return { content: next, message };
}

function unlockDiscoveries(content, selectedRegions) {
  const m = /"persistentProfileData"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"persistentProfileData" not found.');
  }
  const ppBlock = extractBraceBlock(content, m.index);
  const pp = JSON.parse(ppBlock.block);
  let dt = pp.discoveredTrucks;
  const ensured = ensureDiscoveredTrucksDefaults(dt);
  dt = ensured.data;
  let updated = 0;
  for (const [mapKey, entry] of Object.entries(dt)) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      continue;
    }
    if (!selectedRegions.some((code) => mapKey.toLowerCase().includes(code.toLowerCase()))) {
      continue;
    }
    setCurrentToAll(entry);
    updated += 1;
  }
  pp.discoveredTrucks = dt;

  const newBlock = JSON.stringify(pp);
  const next = content.slice(0, ppBlock.start) + newBlock + content.slice(ppBlock.end);
  let message = `Updated ${updated} discovery entries.`;
  if (ensured.added > 0) {
    message += ` Added ${ensured.added} missing entries.`;
  }
  return { content: next, message };
}

function unlockLevels(content, selectedRegions) {
  const m = /"persistentProfileData"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"persistentProfileData" not found.');
  }
  const ppBlock = extractBraceBlock(content, m.index);
  const pp = JSON.parse(ppBlock.block);

  let known = Array.isArray(pp.knownRegions) ? [...pp.knownRegions] : [];
  let addedKnown = 0;
  for (const code of selectedRegions) {
    const key = code.toLowerCase();
    if (!known.includes(key)) {
      known.push(key);
      addedKnown += 1;
    }
  }
  pp.knownRegions = known;

  content = content.slice(0, ppBlock.start) + JSON.stringify(pp) + content.slice(ppBlock.end);

  const visitedMatch = /"visitedLevels"\s*:\s*\[/i.exec(content);
  let visited = [];
  let visitedBlock = null;
  if (visitedMatch) {
    visitedBlock = extractBracketBlock(content, visitedMatch.index);
    visited = JSON.parse(visitedBlock.block);
    if (!Array.isArray(visited)) {
      visited = [];
    }
  }

  let addedVisited = 0;
  for (const code of selectedRegions) {
    const levels = REGION_LEVELS[code] || [];
    for (const levelId of levels) {
      if (!visited.includes(levelId)) {
        visited.push(levelId);
        addedVisited += 1;
      }
    }
  }

  if (visitedBlock) {
    content = content.slice(0, visitedBlock.start) + JSON.stringify(visited) + content.slice(visitedBlock.end);
  } else {
    content = insertKeyAtRoot(content, "visitedLevels", JSON.stringify(visited));
  }

  return {
    content,
    message: `Known regions added: ${addedKnown}. Visited levels added: ${addedVisited}.`,
  };
}

function unlockGarages(content, selectedRegions, upgradeAll) {
  const m = /"SslValue"\s*:\s*\{/i.exec(content);
  if (!m) {
    throw new Error('"SslValue" block not found.');
  }
  const sslBlock = extractBraceBlock(content, m.index);
  const ssl = JSON.parse(sslBlock.block);

  let lgData = ssl.levelGarageStatuses;
  if (!lgData || typeof lgData !== "object" || Array.isArray(lgData)) {
    lgData = {};
  }
  let addedDefaults = 0;
  for (const [levelId, status] of Object.entries(LEVEL_GARAGE_STATUSES_DEFAULTS)) {
    if (!(levelId in lgData)) {
      lgData[levelId] = status;
      addedDefaults += 1;
    }
  }

  const selectedLevels = [];
  for (const code of selectedRegions) {
    const levels = REGION_LEVELS[code] || [];
    for (const levelId of levels) {
      selectedLevels.push(levelId);
    }
  }

  let updated = 0;
  for (const levelId of selectedLevels) {
    if (lgData[levelId] === 1) {
      lgData[levelId] = 2;
      updated += 1;
    }
  }
  ssl.levelGarageStatuses = lgData;

  let garagesData = ssl.garagesData;
  if (!garagesData || typeof garagesData !== "object" || Array.isArray(garagesData)) {
    garagesData = {};
  }
  let addedGaragesData = 0;
  for (const levelId of selectedLevels) {
    if (lgData[levelId] === 2 && !garagesData[levelId]) {
      garagesData[levelId] = buildGarageDataEntry();
      addedGaragesData += 1;
    }
  }
  ssl.garagesData = garagesData;

  let upgradedEntries = 0;
  let addedUpgradable = 0;
  if (upgradeAll) {
    let ug = ssl.upgradableGarages;
    if (!ug || typeof ug !== "object" || Array.isArray(ug)) {
      ug = {};
    }
    for (const levelId of selectedLevels) {
      if (lgData[levelId] !== 2) {
        continue;
      }
      let foundKey = null;
      for (const [key, value] of Object.entries(ug)) {
        if (typeof key === "string" && key.toLowerCase().includes(levelId.toLowerCase())) {
          foundKey = key;
          break;
        }
        if (value && typeof value === "object" && typeof value.zoneGlobalId === "string" && value.zoneGlobalId.toLowerCase().includes(levelId.toLowerCase())) {
          foundKey = key;
          break;
        }
      }
      const key = foundKey || makeUpgradableGarageKey(levelId);
      let entry = ug[key];
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
        entry = {
          zoneGlobalId: key,
          featureStates: [false, false, false, false],
          isUpgradable: true,
        };
        ug[key] = entry;
        if (!foundKey) {
          addedUpgradable += 1;
        }
      }
      if (!entry.zoneGlobalId) {
        entry.zoneGlobalId = key;
      }
      normalizeFeatureStates(entry);
      entry.featureStates = entry.featureStates.map(() => true);
      entry.isUpgradable = true;
      ug[key] = entry;
      upgradedEntries += 1;
    }
    ssl.upgradableGarages = ug;
  }

  const next = content.slice(0, sslBlock.start) + JSON.stringify(ssl) + content.slice(sslBlock.end);
  let message = `Unlocked ${updated} garages.`;
  if (addedDefaults > 0) {
    message += ` Added ${addedDefaults} missing garage status entries.`;
  }
  if (addedGaragesData > 0) {
    message += ` Added ${addedGaragesData} garage data entries.`;
  }
  if (upgradeAll) {
    message += ` Upgraded ${upgradedEntries} garages.`;
    if (addedUpgradable > 0) {
      message += ` Added ${addedUpgradable} upgradable garage entries.`;
    }
  }
  return { content: next, message };
}

function buildGarageDataEntry() {
  return {
    slotsDatas: {
      garage_interior_slot_1: { garageSlotZoneId: "garage_interior_slot_1", truckDesc: null },
      garage_interior_slot_2: { garageSlotZoneId: "garage_interior_slot_2", truckDesc: null },
      garage_interior_slot_3: { garageSlotZoneId: "garage_interior_slot_3", truckDesc: null },
      garage_interior_slot_4: { garageSlotZoneId: "garage_interior_slot_4", truckDesc: null },
      garage_interior_slot_5: { garageSlotZoneId: "garage_interior_slot_5", truckDesc: null },
      garage_interior_slot_6: { garageSlotZoneId: "garage_interior_slot_6", truckDesc: null },
    },
    selectedSlot: "garage_interior_slot_1",
  };
}

function normalizeFeatureStates(entry) {
  let states = Array.isArray(entry.featureStates) ? [...entry.featureStates] : [];
  while (states.length < 4) {
    states.push(false);
  }
  states = states.map((v) => Boolean(v));
  entry.featureStates = states;
  if (!("isUpgradable" in entry)) {
    entry.isUpgradable = true;
  }
}

function makeUpgradableGarageKey(levelId) {
  const suffix = String(levelId).replace(/^level_/i, "").toUpperCase();
  return `${levelId} || ${suffix}_GARAGE_ENTRANCE`;
}

function setCurrentToAll(entry) {
  let allVal = entry.all;
  if (typeof allVal === "boolean") {
    allVal = Number(allVal);
  } else if (typeof allVal !== "number") {
    const parsed = Number.parseInt(String(allVal), 10);
    allVal = Number.isFinite(parsed) ? parsed : 0;
  }
  entry.all = allVal;
  entry.current = allVal;
}

function parseCommonContainer(text) {
  const m = /"CommonSslSave"\s*:\s*\{/i.exec(text);
  if (m) {
    const block = extractBraceBlock(text, m.index);
    const parsed = JSON.parse(block.block);
    return { parsed, embedded: true, start: block.start, end: block.end };
  }
  const parsed = JSON.parse(text);
  return { parsed, embedded: false, start: null, end: null };
}

function writeCommonContainer(originalText, container) {
  const serialized = JSON.stringify(container.parsed);
  if (container.embedded) {
    return originalText.slice(0, container.start) + serialized + originalText.slice(container.end);
  }
  return serialized;
}

function getCommonSslValue(parsed) {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Invalid CommonSslSave JSON.");
  }
  if (parsed.SslValue && typeof parsed.SslValue === "object" && !Array.isArray(parsed.SslValue)) {
    return parsed.SslValue;
  }
  return parsed;
}

function readFinishedTrials(text) {
  const m = /"finishedTrials"\s*:\s*\[/i.exec(text);
  if (!m) {
    return [];
  }
  try {
    const block = extractBracketBlock(text, m.index);
    const arr = JSON.parse(block.block);
    return Array.isArray(arr) ? arr : [];
  } catch (_err) {
    return [];
  }
}

function writeFinishedTrials(text, finishedList) {
  const arrText = JSON.stringify(finishedList);
  const m = /"finishedTrials"\s*:\s*\[/i.exec(text);
  if (m) {
    try {
      const block = extractBracketBlock(text, m.index);
      return `${text.slice(0, block.start)}${arrText}${text.slice(block.end)}`;
    } catch (_error) {
      // fall back to insert if malformed
    }
  }
  const firstBrace = text.indexOf("{");
  if (firstBrace === -1) {
    return `${text}\n"finishedTrials":${arrText}\n`;
  }
  return `${text.slice(0, firstBrace + 1)}\n"finishedTrials":${arrText},${text.slice(firstBrace + 1)}`;
}

function replaceOrInsertNumeric(content, key, value) {
  const replaced = replaceNumericKeyAll(content, key, value);
  if (replaced.count > 0) {
    return replaced.content;
  }
  return insertKeyAtRoot(content, key, JSON.stringify(value));
}

function replaceOrInsertNumericWithCount(content, key, value) {
  const replaced = replaceNumericKeyAll(content, key, value);
  if (replaced.count > 0) {
    return replaced;
  }
  return {
    content: insertKeyAtRoot(content, key, JSON.stringify(value)),
    count: 1,
  };
}

function replaceOrInsertJsonLiteral(content, key, jsonValueText) {
  const replaced = replaceJsonKeyAll(content, key, jsonValueText);
  if (replaced.count > 0) {
    return replaced.content;
  }
  return insertKeyAtRoot(content, key, jsonValueText);
}

function updateAllContestTimesBlocks(content, newEntries) {
  const matches = [...content.matchAll(/"contestTimes"\s*:\s*\{/gi)];
  let out = content;
  for (let i = matches.length - 1; i >= 0; i -= 1) {
    const hit = matches[i];
    let block = null;
    try {
      block = extractBraceBlock(out, hit.index);
    } catch (_error) {
      continue;
    }
    let parsed = null;
    try {
      parsed = JSON.parse(block.block);
    } catch (_error) {
      continue;
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      continue;
    }
    let changed = false;
    for (const [key, value] of Object.entries(newEntries)) {
      if (!(key in parsed)) {
        parsed[key] = value;
        changed = true;
      }
    }
    if (!changed) {
      continue;
    }
    const nextBlock = JSON.stringify(parsed);
    out = out.slice(0, block.start) + nextBlock + out.slice(block.end);
  }
  return out;
}

function replaceOrInsertBoolean(content, key, value) {
  const replaced = replaceBooleanKeyAll(content, key, value);
  if (replaced.count > 0) {
    return replaced.content;
  }
  return insertKeyAtRoot(content, key, JSON.stringify(Boolean(value)));
}

function replaceNumericKeyAll(content, key, value) {
  let count = 0;
  const re = new RegExp(
    `("${escapeRegExp(key)}"\\s*:\\s*)(?:"(?:[^"\\\\]|\\\\.)*"|-?\\d+(?:\\.\\d+)?(?:e[-+]?\\d+)?)`,
    "gi",
  );
  const out = content.replace(re, (_, p1) => {
    count += 1;
    return `${p1}${value}`;
  });
  return { content: out, count };
}

function replaceJsonKeyAll(content, key, jsonValueText) {
  let count = 0;
  const valuePattern = `(?:\"(?:[^\"\\\\]|\\\\.)*\"|\\[[^\\]]*\\]|\\{[^\\}]*\\}|[^,}\\n\\r]+)`;
  const re = new RegExp(`("${escapeRegExp(key)}"\\s*:\\s*)${valuePattern}`, "gi");
  const out = content.replace(re, (_, p1) => {
    count += 1;
    return `${p1}${jsonValueText}`;
  });
  return { content: out, count };
}

function replaceBooleanKeyAll(content, key, value) {
  let count = 0;
  const re = new RegExp(`("${escapeRegExp(key)}"\\s*:\\s*)(true|false)`, "gi");
  const out = content.replace(re, (_, p1) => {
    count += 1;
    return `${p1}${value ? "true" : "false"}`;
  });
  return { content: out, count };
}

function insertKeyAtRoot(content, key, jsonValueText) {
  const idx = content.indexOf("{");
  if (idx === -1) {
    return content;
  }
  return `${content.slice(0, idx + 1)}"${key}":${jsonValueText},${content.slice(idx + 1)}`;
}

function parseOptionalInt(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalStrictInt(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return { value: null, error: null };
  }
  if (!/^[+-]?\d+$/.test(trimmed)) {
    return { value: null, error: "invalid" };
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isSafeInteger(parsed)) {
    return { value: null, error: "invalid" };
  }
  return { value: parsed, error: null };
}

function parseOptionalMoneyInt(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return { value: null, error: null, clamped: false };
  }
  if (!/^[+-]?\d+$/.test(trimmed)) {
    return { value: null, error: "invalid", clamped: false };
  }
  let raw = null;
  try {
    raw = BigInt(trimmed);
  } catch (_error) {
    return { value: null, error: "invalid", clamped: false };
  }
  const min = BigInt(MONEY_MIN);
  const max = BigInt(MONEY_MAX);
  let clampedValue = raw;
  if (clampedValue < min) {
    clampedValue = min;
  } else if (clampedValue > max) {
    clampedValue = max;
  }
  return {
    value: Number(clampedValue),
    error: null,
    clamped: clampedValue !== raw,
  };
}

function parseOptionalFloat(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return null;
  }
  return parseStrictNumber(trimmed);
}

function parseStrictNumber(value) {
  const trimmed = String(value == null ? "" : value).trim();
  if (!trimmed) {
    return null;
  }
  if (!/^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(trimmed)) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalStrictFloat(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return { value: null, error: null };
  }
  const parsed = parseStrictNumber(trimmed);
  if (parsed == null) {
    return { value: null, error: "invalid" };
  }
  return { value: parsed, error: null };
}

function readMaxIntKey(content, key) {
  const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(-?\\d+)`, "gi");
  let m = null;
  let max = null;
  while ((m = re.exec(content)) !== null) {
    const num = Number.parseInt(m[1], 10);
    if (!Number.isFinite(num)) {
      continue;
    }
    if (max == null || num > max) {
      max = num;
    }
  }
  return max;
}

function readNumericKey(content, key) {
  const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:e[-+]?\\d+)?)`, "i");
  const m = re.exec(content);
  if (!m) {
    return null;
  }
  const value = Number.parseFloat(m[1]);
  return Number.isFinite(value) ? value : null;
}

function readBoolKey(content, key) {
  const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(true|false)`, "i");
  const m = re.exec(content);
  if (!m) {
    return null;
  }
  return m[1].toLowerCase() === "true";
}

function readSimpleValueKey(content, key) {
  const re = new RegExp(`"${escapeRegExp(key)}"\\s*:\\s*(true|false|-?\\d+(?:\\.\\d+)?(?:e[-+]?\\d+)?)`, "i");
  const m = re.exec(content);
  if (!m) {
    return undefined;
  }
  const raw = m[1].toLowerCase();
  if (raw === "true") {
    return true;
  }
  if (raw === "false") {
    return false;
  }
  const num = Number.parseFloat(m[1]);
  return Number.isFinite(num) ? num : undefined;
}

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractBraceBlock(text, startIndex) {
  let openBraces = 0;
  let inString = false;
  let escape = false;
  let blockStart = null;
  for (let i = startIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '"' && !escape) {
      inString = !inString;
    }
    if (!inString) {
      if (ch === "{") {
        if (openBraces === 0) {
          blockStart = i;
        }
        openBraces += 1;
      } else if (ch === "}") {
        openBraces -= 1;
        if (openBraces === 0 && blockStart != null) {
          return {
            block: text.slice(blockStart, i + 1),
            start: blockStart,
            end: i + 1,
          };
        }
      }
    }
    escape = ch === "\\" && !escape;
  }
  throw new Error("Matching closing brace not found.");
}

function extractBracketBlock(text, startIndex) {
  let open = 0;
  let inString = false;
  let escape = false;
  let blockStart = null;
  for (let i = startIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '"' && !escape) {
      inString = !inString;
    }
    if (!inString) {
      if (ch === "[") {
        if (open === 0) {
          blockStart = i;
        }
        open += 1;
      } else if (ch === "]") {
        open -= 1;
        if (open === 0 && blockStart != null) {
          return {
            block: text.slice(blockStart, i + 1),
            start: blockStart,
            end: i + 1,
          };
        }
      }
    }
    escape = ch === "\\" && !escape;
  }
  throw new Error("Matching closing bracket not found.");
}

function uniqueList(list) {
  return [...new Set(list)];
}

init();
