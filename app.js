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
      { label: "default", value: 1 },
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
const EMBEDDED_OBJECTIVES_CSV = "key,displayName,category,region,region_name,type,cargo_needed,experience,money,descriptionText,Source\nUS_01_01_DROWNED_TRUCK_01_TSK,US_01_01_DROWNED_TRUCK_01_TSK,_TASKS,US_01,Michigan,truckDelivery,,190,1250,US_01_01_DROWNED_TRUCKS_DESC,TASKS\nUS_01_01_DROWNED_TRUCK_02_TSK,US_01_01_DROWNED_TRUCK_02_TSK,_TASKS,US_01,Michigan,truckDelivery,,180,1100,US_01_01_DROWNED_TRUCKS_DESC,TASKS\nUS_01_01_DROWNED_TRUCK_03_TSK,US_01_01_DROWNED_TRUCK_03_TSK,_TASKS,US_01,Michigan,truckDelivery,,320,3000,US_01_01_DROWNED_TRUCKS_DESC,TASKS\nUS_01_01_DRILLING_RECOVERY_OBJ,US_01_01_DRILLING_RECOVERY_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Metal Planks; 1\u00d7 Big Drills,740,7450,US_01_01_DRILLING_RECOVERY_DESC,CONTRACTS\nUS_01_01_FACTORY_RECOVERY_OBJ,US_01_01_FACTORY_RECOVERY_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Bricks; 1\u00d7 Metal Planks,450,3850,US_01_01_FACTORY_RECOVERY_DESC,CONTRACTS\nUS_01_01_TOWN_STORAGE_OBJ,US_01_01_TOWN_STORAGE_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Crate Large; 2\u00d7 Wooden Planks,260,2800,US_01_01_TOWN_STORAGE_OBJ_DESC,CONTRACTS\nUS_01_01_FARM_DELIVERY_OBJ,US_01_01_FARM_DELIVERY_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,,130,1050,US_01_01_FARM_DELIVERY_DESC,CONTRACTS\nUS_01_01_SUPPLIES_FOR_FARMERS_OBJ,US_01_01_SUPPLIES_FOR_FARMERS_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Bricks; 2\u00d7 Wooden Planks; 1\u00d7 Metal Planks,420,3800,US_01_01_SUPPLIES_FOR_FARMERS_DESC,CONTRACTS\nUS_01_01_BUILD_A_BRIDGE_OBJ,US_01_01_BUILD_A_BRIDGE_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Wooden Planks,350,2100,US_01_01_BUILD_A_BRIDGE_DESC,CONTRACTS\nUS_01_01_LOST_CONTAINERS_OBJ,US_01_01_LOST_CONTAINERS_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Service Spare Parts Special,210,1300,US_01_01_LOST_CONTAINERS_DESC,CONTRACTS\nUS_01_01_EXPLORE_GARAGE_OBJ,US_01_01_EXPLORE_GARAGE_OBJ,_CONTRACTS,US_01,Michigan,exploration,,200,850,US_01_01_EXPLORE_GARAGE_DESC,CONTRACTS\nUS_01_01_EXPLORING_TRUCK_OBJ,US_01_01_EXPLORING_TRUCK_OBJ,_CONTRACTS,US_01,Michigan,exploration,,200,650,US_01_01_EXPLORING_TRUCK_DESC,CONTRACTS\nUS_01_01_EXPLORING_WATCHTOWER_OBJ,US_01_01_EXPLORING_WATCHTOWER_OBJ,_CONTRACTS,US_01,Michigan,exploration,,100,250,US_01_01_EXPLORING_WATCHTOWER_DESC,CONTRACTS\nUS_01_01_MISSED_OILTANK_TSK,US_01_01_MISSED_OILTANK_TSK,_TASKS,US_01,Michigan,truckDelivery,,280,2400,US_01_01_MISSED_OILTANK_TSK_DESC_DESC,TASKS\nUS_01_01_MOTEL_NEEDS_TSK,US_01_01_MOTEL_NEEDS_TSK,_TASKS,US_01,Michigan,truckDelivery,,210,1850,US_01_01_MOTEL_NEEDS_DESC,TASKS\nUS_01_01_STUCK_TRAILER_TSK,US_01_01_STUCK_TRAILER_TSK,_TASKS,US_01,Michigan,truckDelivery,,240,1600,US_01_01_STUCK_TRAILER_TSK_DESC_DESC,TASKS\nUS_01_01_LOST_CARGO_TSK,US_01_01_LOST_CARGO_TSK,_TASKS,US_01,Michigan,cargoDelivery,4\u00d7 Service Spare Parts Special,280,3200,US_01_01_LOST_CARGO_TSK_DESC_B,TASKS\nUS_01_01_MOUNTAIN_BRIDGE_TSK,US_01_01_MOUNTAIN_BRIDGE_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Blocks,260,1850,US_01_01_MOUNTAIN_BRIDGE_TSK_DESC,TASKS\nUS_01_01_ROAD_BLOCKAGE_TSK,US_01_01_ROAD_BLOCKAGE_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Service Spare Parts,170,1650,US_01_01_ROAD_BLOCKAGE_DESC,TASKS\nUS_01_01_BOATMAN_TOOLS_DELIVERY_TSK,US_01_01_BOATMAN_TOOLS_DELIVERY_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Wooden Planks,180,1600,US_01_01_BOATMAN__TOOLS_DELIVERY_TSK_DESC,TASKS\nUS_01_01_LANDSLIDE_TSK,US_01_01_LANDSLIDE_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts,180,1600,US_01_01_LANDSLIDE_DESC,TASKS\nUS_01_01_FALLEN_POWER_LINES_TSK,US_01_01_FALLEN_POWER_LINES_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Blocks,200,1550,US_01_01_FALLEN_POWER_LINES_TSK_DESC,TASKS\nUS_01_01_WOODEN_BRIDGE_TSK,US_01_01_WOODEN_BRIDGE_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,120,1400,US_01_01_WOODEN_BRIDGE_DESC,TASKS\nUS_01_01_SWAMP_EXPLORATION_TSK,US_01_01_SWAMP_EXPLORATION_TSK,_TASKS,US_01,Michigan,exploration,,180,3050,US_01_01_SWAMP_EXPLORATION_DESC,TASKS\nUS_01_01_KING_OF_HILLS_TSK,US_01_01_KING_OF_HILLS_TSK,_TASKS,US_01,Michigan,exploration,,160,2800,US_01_01_KING_OF_HILLS_DESC,TASKS\nUS_01_01_LOCAL_ENTERTAINMENT_TSK,US_01_01_LOCAL_ENTERTAINMENT_TSK,_TASKS,US_01,Michigan,exploration,,130,2200,US_01_01_LOCAL_ENTERTAINMENT_DESC,TASKS\nUS_01_01_THE_PLACE_BEYOND_THE_SPRUCES_TSK,US_01_01_THE_PLACE_BEYOND_THE_SPRUCES_TSK,_TASKS,US_01,Michigan,exploration,,60,1050,US_01_01_THE_PLACE_BEYOND_THE_SPRUCES_DESC,TASKS\nUS_01_01_FOOD_DELIVERY_CNT,US_01_01_FOOD_DELIVERY_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,2\u00d7 Crate Large,100,1050,US_01_01_FOOD_DELIVERY_CNT_DESC_DESC,CONTESTS\nUS_01_01_WOODEN_ORDER_CNT,US_01_01_WOODEN_ORDER_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Wooden Planks; 1\u00d7 Wooden Planks,110,950,US_01_01_WOODEN_ORDER_CNT_DESC_DESC,CONTESTS\nUS_01_01_METEO_DATA_CNT,US_01_01_METEO_DATA_CNT,_CONTESTS,US_01,Michigan,exploration,,70,1100,US_01_01_METEO_DATA_CNT_DESC_DESC,CONTESTS\nUS_01_02_CLEAR_ROCKS_01_TSK,US_01_02_CLEAR_ROCKS_01_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Service Spare Parts,160,1500,US_01_02_CLEAR_ROCKS_01_DESC,TASKS\nUS_01_02_RESOURCES_FOR_WINTER_OBJ,US_01_02_RESOURCES_FOR_WINTER_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Vehicles Spare Parts,420,5000,US_01_02_RESOURCES_FOR_WINTER_OBJ_DESC,CONTRACTS\nUS_01_02_MATERIALS_ORDER_OBJ,US_01_02_MATERIALS_ORDER_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Concrete Slab; 2\u00d7 Crate Large; 1\u00d7 Metal Planks,730,5000,US_01_02_MATERIALS_ORDER_DESC,CONTRACTS\nUS_01_02_FARM_ORDER_OBJ,US_01_02_FARM_ORDER_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Bricks; 2\u00d7 Wooden Planks; 2\u00d7 Barrels,440,4100,US_01_02_FARM_ORDER_DESC,CONTRACTS\nUS_01_02_WORK_FOR_OLD_SWEAT_OBJ,US_01_02_WORK_FOR_OLD_SWEAT_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Concrete Slab; 2\u00d7 Service Spare Parts,380,3450,US_01_02_WORK_FOR_OLD_SWEAT_DESC,CONTRACTS\nUS_01_02_FUEL_ORDER_OBJ,US_01_02_FUEL_ORDER_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Container Small,280,2900,US_01_02_FUEL_ORDER_DESC,CONTRACTS\nUS_01_02_BRICKS_DELIVERY_TSK,US_01_02_BRICKS_DELIVERY_TSK,_TASKS,US_01,Michigan,truckDelivery,,270,2450,US_01_02_BRICKS_DELIVERY_DESC,TASKS\nUS_01_02_LOST_TRAILER_TSK,US_01_02_LOST_TRAILER_TSK,_TASKS,US_01,Michigan,truckDelivery,,180,1900,US_01_02_LOST_TRAILER_DESC_DESC,TASKS\nUS_01_02_UNLUCKY_FISHERMAN_TSK,US_01_02_UNLUCKY_FISHERMAN_TSK,_TASKS,US_01,Michigan,truckDelivery,,250,1850,US_01_02_UNLUCKY_FISHERMAN_DESC,TASKS\nUS_01_02_RIVER_CROSSING_TSK,US_01_02_RIVER_CROSSING_TSK,_TASKS,US_01,Michigan,truckDelivery,,150,1350,US_01_02_RIVER_CROSSING_TSK_DESC_DESC,TASKS\nUS_01_02_TRUCK_RESTORATION_TSK,US_01_02_TRUCK_RESTORATION_TSK,_TASKS,US_01,Michigan,truckDelivery,,140,1200,US_01_02_TRUCK_RESTORATION_TSK_DESC_DESC,TASKS\nUS_01_02_CLEAN_THE_RIVER_EAST_TSK,US_01_02_CLEAN_THE_RIVER_EAST_TSK,_TASKS,US_01,Michigan,cargoDelivery,4\u00d7 Barrels Oil,340,3500,US_01_02_CLEAN_THE_RIVER_EAST_DESC,TASKS\nUS_01_02_CLEAN_THE_RIVER_WEST_TSK,US_01_02_CLEAN_THE_RIVER_WEST_TSK,_TASKS,US_01,Michigan,cargoDelivery,3\u00d7 Barrels Oil,310,3100,US_01_02_CLEAN_THE_RIVER_WEST_DESC,TASKS\nUS_01_02_SOLID_FOUNDATION_TSK,US_01_02_SOLID_FOUNDATION_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Concrete Slab,400,3000,US_01_02_SOLID_FOUNDATION_DESC,TASKS\nUS_01_02_LOST_BAGS_TSK,US_01_02_LOST_BAGS_TSK,_TASKS,US_01,Michigan,cargoDelivery,4\u00d7 Bags,330,2900,US_01_02_LOST_BAGS_TSK_DESC,TASKS\nUS_01_02_FOOD_FOR_WORKERS_TSK,US_01_02_FOOD_FOR_WORKERS_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Crate Large,240,2400,US_01_02_FOOD_FOR_WORKERS_DESC,TASKS\nUS_01_02_WOODEN_BRIDGE_TSK,US_01_02_WOODEN_BRIDGE_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,200,2200,US_01_02_WOODEN_BRIDGE_DESC,TASKS\nUS_01_02_DRILL_FOR_OUTCAST_TSK,US_01_02_DRILL_FOR_OUTCAST_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Vehicles Spare Parts,210,2100,US_01_02_DRILL_FOR_OUTCAST_DESC,TASKS\nUS_01_02_BARRELS_DELIVERY_TSK,US_01_02_BARRELS_DELIVERY_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Barrels,180,1900,US_01_02_BARRELS_DELIVERY_DESC,TASKS\nUS_01_02_FALLEN_ROCKS_TSK,US_01_02_FALLEN_ROCKS_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Service Spare Parts,170,1300,US_01_02_FALLEN_ROCKS_DESC,TASKS\nUS_01_02_REPAIR_THE_TRUCK_TSK,US_01_02_REPAIR_THE_TRUCK_TSK,_TASKS,US_01,Michigan,exploration,,180,1900,US_01_02_REPAIR_THE_TRUCK_DESC,TASKS\nUS_01_02_FIND_THE_ANTENNA_TOWER_TSK,US_01_02_FIND_THE_ANTENNA_TOWER_TSK,_TASKS,US_01,Michigan,exploration,,80,1400,US_01_02_FIND_THE_ANTENNA_TOWER_DESC,TASKS\nUS_01_02_MICHIGAN_TRIAL_TSK,US_01_02_MICHIGAN_TRIAL_TSK,_TASKS,US_01,Michigan,exploration,,80,1350,US_01_02_MICHIGAN_TRIAL_DESC,TASKS\nUS_01_02_FLOODED_HOUSE_CNT,US_01_02_FLOODED_HOUSE_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,2\u00d7 Blocks,150,1250,US_01_02_FLOODED_HOUSE_DESC,CONTESTS\nUS_01_02_HOUSE_RENOVATION_CNT,US_01_02_HOUSE_RENOVATION_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,2\u00d7 Bricks,110,1050,US_01_02_HOUSE_RENOVATION_DESC,CONTESTS\nUS_01_02_FARMERS_NEEDS_CNT,US_01_02_FARMERS_NEEDS_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,3\u00d7 Service Spare Parts,110,900,US_01_02_FARMERS_NEEDS_DESC,CONTESTS\nUS_01_03_POWER_WIRES_1_CONTRACT_OBJ,US_01_03_POWER_WIRES_1_CONTRACT_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Metal Planks; 1\u00d7 Metal Planks,610,4400,US_01_03_POWER_WIRES_1_CONTRACT_OBJ_DESC,CONTRACTS\nUS_01_03_DROPPED_VEHICLE_SEARCHING_TSK_01,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_01,_TASKS,US_01,Michigan,truckDelivery,,140,1100,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_01_DESC,TASKS\nUS_01_03_SWAMP_CROSSING_01_TSK,US_01_03_SWAMP_CROSSING_01_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,100,1000,US_01_03_SWAMP_CROSSING_01_TSK_DESC,TASKS\nUS_01_03_DROPPED_VEHICLE_SEARCHING_TSK_02,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_02,_TASKS,US_01,Michigan,truckDelivery,,160,1500,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_02_DESC,TASKS\nUS_01_03_SWAMP_CROSSING_02_TSK,US_01_03_SWAMP_CROSSING_02_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,100,900,US_01_03_SWAMP_CROSSING_02_TSK_DESC,TASKS\nUS_01_03_SWAMP_CROSSING_03_TSK,US_01_03_SWAMP_CROSSING_03_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,110,1150,US_01_03_SWAMP_CROSSING_03_TSK_DESC,TASKS\nUS_01_03_DROPPED_VEHICLE_SEARCHING_TSK_03,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_03,_TASKS,US_01,Michigan,exploration,,70,1250,US_01_03_DROPPED_VEHICLE_SEARCHING_TSK_03_DESC,TASKS\nUS_01_03_LUMBER_MILL_REACTIVATION_OBJ,US_01_03_LUMBER_MILL_REACTIVATION_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Blocks; 1\u00d7 Metal Planks,590,4800,US_01_03_LUMBER_MILL_REACTIVATION_DESC,CONTRACTS\nUS_01_03_LOST_CARGO_TSK,US_01_03_LOST_CARGO_TSK,_CONTRACTS,US_01,Michigan,cargoDelivery,3\u00d7 Container Large Drilling,770,5950,US_01_03_LOST_CARGO_DESC,CONTRACTS\nUS_01_03_CARGO_PORT_OBJ,US_01_03_CARGO_PORT_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Container Large Drilling,590,4750,US_01_03_CARGO_PORT_OBJ_DESC,CONTRACTS\nUS_01_03_SHORT_CUT_TSK,US_01_03_SHORT_CUT_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Wooden Planks,140,1700,US_01_03_SHORT_CUT_TSK_DESC,TASKS\nUS_01_03_FIX_THE_ANTENNA_TSK,US_01_03_FIX_THE_ANTENNA_TSK,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Container Small,140,1200,US_01_03_FIX_THE_ANTENNA_TSK_DESC,TASKS\nUS_01_03_TRUCK_REPAIR,US_01_03_TRUCK_REPAIR,_TASKS,US_01,Michigan,exploration,,160,1500,US_01_03_TRUCK_REPAIR_DESC,TASKS\nUS_01_03_FIND_THE_ANTENNA_TSK,US_01_03_FIND_THE_ANTENNA_TSK,_TASKS,US_01,Michigan,exploration,,40,600,US_01_03_FIND_THE_ANTENNA_TSK_DESC,TASKS\nUS_01_03_BARREL_CNT,US_01_03_BARREL_CNT,_CONTESTS,US_01,Michigan,cargoDelivery,1\u00d7 Barrels Oil,110,950,US_01_03_BARREL_CNT_DESC,CONTESTS\nUS_01_04_BUILD_A_BRIDGE_OBJ_1,US_01_04_BUILD_A_BRIDGE_OBJ_1,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,100,1000,US_01_04_BUILD_A_BRIDGE_OBJ_1_DESC,TASKS\nUS_01_04_SUPPLIES_FOR_REGION_2_OBJ,US_01_04_SUPPLIES_FOR_REGION_2_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Bags; 2\u00d7 Bags; 2\u00d7 Bags; 2\u00d7 Bags,2000,22350,US_01_04_SUPPLIES_FOR_REGION_DESC,CONTRACTS\nUS_01_04_MED_LOGS_2_OBJ,US_01_04_MED_LOGS_2_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,3\u00d7 Logs Medium; 2\u00d7 Logs Long; 1\u00d7 Logs Medium; 1\u00d7 Logs Long; 2\u00d7 Logs Medium,1850,17250,US_01_04_MED_LOGS_2_DESC,CONTRACTS\nUS_01_04_LONG_LOGS_2_OBJ,US_01_04_LONG_LOGS_2_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Logs Long; 1\u00d7 Logs Long,1030,9600,US_01_04_LONG_LOGS_2_DESC,CONTRACTS\nUS_01_04_BUILD_A_BRIDGE_OBJ_2,US_01_04_BUILD_A_BRIDGE_OBJ_2,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Wooden Planks,100,1000,US_01_04_BUILD_A_BRIDGE_TSK_2_DESC,TASKS\nUS_01_04_BUILD_A_BRIDGE_OBJ_3,US_01_04_BUILD_A_BRIDGE_OBJ_3,_TASKS,US_01,Michigan,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Concrete Slab; 1\u00d7 Metal Planks; 1\u00d7 Blocks,640,6000,US_01_04_BUILD_A_BRIDGE_OBJ_3_DESC,TASKS\nUS_01_04_CARGO_DELIVERING_OBJ,US_01_04_CARGO_DELIVERING_OBJ,_CONTRACTS,US_01,Michigan,truckDelivery,2\u00d7 Metal Planks; 1\u00d7 Big Drills,1180,11000,US_01_04_CARGO_DELIVERING_DESC,CONTRACTS\nUS_01_04_MULTIORDER_OBJ,US_01_04_MULTIORDER_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Barrels; 1\u00d7 Crate Large; 1\u00d7 Service Spare Parts; 1\u00d7 Wooden Planks; 1\u00d7 Barrels; 1\u00d7 Crate Large; 1\u00d7 Blocks; 1\u00d7 Vehicles Spare Parts; 1\u00d7 Barrels; 1\u00d7 Metal Planks; 1\u00d7 Concrete Slab,2740,32650,US_01_04_MULTIORDER_DESC,CONTRACTS\nUS_01_04_LONG_LOGS_OBJ,US_01_04_LONG_LOGS_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,3\u00d7 Logs Long; 3\u00d7 Logs Medium; 3\u00d7 Logs Medium,2060,22450,US_01_04_LONG_LOGS_DESC,CONTRACTS\nUS_01_04_CARGO_FROM_SHIP_OBJ,US_01_04_CARGO_FROM_SHIP_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,2\u00d7 Container Large,1270,14100,US_01_04_CARGO_FROM_SHIP_DESC,CONTRACTS\nUS_01_04_MED_LOGS_OBJ,US_01_04_MED_LOGS_OBJ,_CONTRACTS,US_01,Michigan,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Medium; 2\u00d7 Logs Medium,990,11300,US_01_04_MED_LOGS_DESC,CONTRACTS\nUS_01_04_FIND_LOST_TRUCK_TSK,US_01_04_FIND_LOST_TRUCK_TSK,_TASKS,US_01,Michigan,truckDelivery,,140,1200,US_01_04_FIND_LOST_TRUCK_TSK_DESC,TASKS\nUS_01_04_FALLEN_CARGO_TSK,US_01_04_FALLEN_CARGO_TSK,_TASKS,US_01,Michigan,cargoDelivery,2\u00d7 Barrels Oil,340,2150,US_01_04_FALLEN_CARGO_TSK_DESC,TASKS\nUS_01_04_LOST_CARGO_DELIVERY_TSK,US_01_04_LOST_CARGO_DELIVERY_TSK,_TASKS,US_01,Michigan,cargoDelivery,3\u00d7 Container Small Special,240,2150,US_01_04_LOST_CARGO_DESC,TASKS\nUS_01_04_LOST_SHIP_OBJ,US_01_04_LOST_SHIP_OBJ,_TASKS,US_01,Michigan,exploration,,50,850,US_01_04_LOST_SHIP_OBJ_DESC,TASKS\nUS_01_04_OBSERVATION_DECK_TSK,US_01_04_OBSERVATION_DECK_TSK,_TASKS,US_01,Michigan,exploration,,40,650,US_01_04_OBSERVATION_DECK_TSK_DESC,TASKS\nUS_01_04_PATH__PASSING_TSK,US_01_04_PATH__PASSING_TSK,_TASKS,US_01,Michigan,exploration,,30,500,US_01_04_PATH__PASSING_TSK_DESC,TASKS\nUS_01_04_EXPLORING_CNT,US_01_04_EXPLORING_CNT,_CONTESTS,US_01,Michigan,exploration,,40,600,US_01_04_EXPLORING_CNT_DESC,CONTESTS\nUS_02_01_LOGS_01_OBJ,US_02_01_LOGS_01_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Logs Medium; 1\u00d7 Logs Long,624,5670,US_02_01_LOGS_01_OBJ_DESC,CONTRACTS\nUS_02_01_MOUNTAIN_CONQUEST_1_CNT,US_02_01_MOUNTAIN_CONQUEST_1_CNT,_CONTESTS,US_02,Alaska,exploration,,50,750,US_02_01_MOUNTAIN_CONQUEST_1_CNT_DESC,CONTESTS\nUS_02_01_OIL_DELIVERY_02_OBJ,US_02_01_OIL_DELIVERY_02_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Barrels Oil,290,2500,US_02_01_OIL_DELIVERY_02_OBJ_DESC,CONTRACTS\nUS_02_01_MOUNTAIN_CONQUEST_2_CNT,US_02_01_MOUNTAIN_CONQUEST_2_CNT,_CONTESTS,US_02,Alaska,exploration,,40,650,US_02_01_MOUNTAIN_CONQUEST_2_CNT_DESC,CONTESTS\nUS_02_01_DISASS_OBJ,US_02_01_DISASS_OBJ,_CONTRACTS,US_02,Alaska,truckDelivery,,360,3350,US_02_01_DISASS_OBJ_DESC,CONTRACTS\nUS_02_01_DRILL_DELIVERY_OBJ,US_02_01_DRILL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Container Small; 1\u00d7 Pipes Medium,670,5900,US_02_01_DRILL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_01_OIL_DELIVERY_OBJ,US_02_01_OIL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil,520,5600,US_02_01_OIL_DELIVERY_DESC,CONTRACTS\nUS_02_01_SPECIAL_DELIVERY_OBJ,US_02_01_SPECIAL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Service Spare Parts Special,470,4400,US_02_01_SPECIAL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_01_POLAR_BASE_OBJ,US_02_01_POLAR_BASE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Bricks; 1\u00d7 Metal Planks; 2\u00d7 Barrels,390,3450,US_02_01_POLAR_BASE_OBJ_DESC,CONTRACTS\nUS_02_01_BARRELS_OBJ,US_02_01_BARRELS_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Barrels,250,2750,US_02_01_BARRELS_OBJ_DESC,CONTRACTS\nUS_02_01_PIPELINE_OBJ,US_02_01_PIPELINE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Service Spare Parts,180,1400,US_02_01_PIPELINE_OBJ_DESC,CONTRACTS\nUS_02_01_CANT_GO_TO_WASTE_TSK,US_02_01_CANT_GO_TO_WASTE_TSK,_TASKS,US_02,Alaska,truckDelivery,,350,2250,US_02_01_CANT_GO_TO_WASTE_TSK_DESC,TASKS\nUS_02_01_OILTANK_DELIVERY_TSK,US_02_01_OILTANK_DELIVERY_TSK,_TASKS,US_02,Alaska,truckDelivery,,300,2100,US_02_01_OILTANK_DELIVERY_TSK_DESC,TASKS\nUS_02_01_STUCK_SCOUT_TSK,US_02_01_STUCK_SCOUT_TSK,_TASKS,US_02,Alaska,truckDelivery,,240,1650,US_02_01_STUCK_SCOUT_TSK_DESC,TASKS\nUS_02_01_SERVICE_RETURN_TSK,US_02_01_SERVICE_RETURN_TSK,_TASKS,US_02,Alaska,truckDelivery,,210,1500,US_02_01_SERVICE_RETURN_TSK_DESC,TASKS\nUS_02_01_HUMMER_TSK,US_02_01_HUMMER_TSK,_TASKS,US_02,Alaska,truckDelivery,,180,1500,US_02_01_HUMMER_TSK_DESC,TASKS\nUS_02_01_LOST_OILTANK_TSK,US_02_01_LOST_OILTANK_TSK,_TASKS,US_02,Alaska,truckDelivery,,150,1000,US_02_01_LOST_OILTANK_TSK_DESC,TASKS\nUS_02_01_ABANDONED_SUPPLIES_TSK,US_02_01_ABANDONED_SUPPLIES_TSK,_TASKS,US_02,Alaska,truckDelivery,,150,950,US_02_01_ABANDONED_SUPPLIES_TSK_DESC,TASKS\nUS_02_01_BAGS_ON_ICE_TSK,US_02_01_BAGS_ON_ICE_TSK,_TASKS,US_02,Alaska,cargoDelivery,4\u00d7 Bags,480,3100,US_02_01_BAGS_ON_ICE_TSK_DESC,TASKS\nUS_02_01_CONTAINERS_IN_RIVER_TSK,US_02_01_CONTAINERS_IN_RIVER_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Container Small,350,2600,US_02_01_CONTAINERS_IN_RIVER_DESC,TASKS\nUS_02_01_LOST_TUBE_TSK,US_02_01_LOST_TUBE_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Pipe Large,360,1950,US_02_01_LOST_TUBE_TSK_DESC,TASKS\nUS_02_01_ROCK_TSK,US_02_01_ROCK_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Metal Planks,180,1750,US_02_01_ROCK_DESC,TASKS\nUS_02_01_FIX_A_BRIDGE_TSK,US_02_01_FIX_A_BRIDGE_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Metal Planks,180,1650,US_02_01_FIX_A_BRIDGE_DESC,TASKS\nUS_02_01_ROCK_FALL_TSK,US_02_01_ROCK_FALL_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Wooden Planks,200,1300,US_02_01_ROCK_FALL_DESC,TASKS\nUS_02_01_STONE_FALL_TSK,US_02_01_STONE_FALL_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Metal Planks,200,1300,US_02_01_STONE_FALL_DESC,TASKS\nUS_02_01_POWERLINE_CHECK_TSK,US_02_01_POWERLINE_CHECK_TSK,_TASKS,US_02,Alaska,exploration,,130,2150,US_02_01_POWERLINE_CHECK_TSK_DESC,TASKS\nUS_02_01_TRAILER_PARK_TSK,US_02_01_TRAILER_PARK_TSK,_TASKS,US_02,Alaska,exploration,,60,950,US_02_01_TRAILER_PARK_TSK_DESC,TASKS\nUS_02_01_RADIOSTATION_TSK,US_02_01_RADIOSTATION_TSK,_TASKS,US_02,Alaska,exploration,,50,900,US_02_01_RADIOSTATION_TSK_DESC,TASKS\nUS_02_01_EMPLOYEE_DISLOCATION_CNT,US_02_01_EMPLOYEE_DISLOCATION_CNT,_CONTESTS,US_02,Alaska,cargoDelivery,1\u00d7 Service Spare Parts,90,950,US_02_01_EMPLOYEE_DISLOCATION_CNT_DESC,CONTESTS\nUS_02_01_FLAGS_CNT,US_02_01_FLAGS_CNT,_CONTESTS,US_02,Alaska,exploration,,30,450,US_02_01_FLAGS_CNT_DESC,CONTESTS\nUS_02_02_LOGS_01_OBJ,US_02_02_LOGS_01_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Logs Medium; 2\u00d7 Logs Long,1440,16650,US_02_02_LOGS_01_OBJ_DESC,CONTRACTS\nUS_02_02_DISASS_OBJ,US_02_02_DISASS_OBJ,_CONTRACTS,US_02,Alaska,truckDelivery,,320,3950,US_02_02_DISASS_OBJ_DESC,CONTRACTS\nUS_02_02_PIPELINE_OBJ,US_02_02_PIPELINE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts; 1\u00d7 Pipes Medium; 1\u00d7 Pipes Small,780,4750,US_02_02_PIPELINE_OBJ_DESC,CONTRACTS\nUS_02_02_VILLAGE_DELIVERY_OBJ,US_02_02_VILLAGE_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Bags; 2\u00d7 Crate Large,460,3550,US_02_02_VILLAGE_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_02_MILL_DELIVERY_OBJ,US_02_02_MILL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Service Spare Parts; 1\u00d7 Metal Planks; 2\u00d7 Barrels; 1\u00d7 Pipes Small,450,3300,US_02_02_MILL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_02_TSTOP_DELIVERY_OBJ,US_02_02_TSTOP_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Crate Large; 4\u00d7 Barrels; 2\u00d7 Wooden Planks,380,3300,US_02_02_TSTOP_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_02_DRILLING_PARTS_OBJ,US_02_02_DRILLING_PARTS_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Container Large Drilling,280,2350,US_02_02_DRILLING_PARTS_OBJ_DESC,CONTRACTS\nUS_02_02_POLAR_BASE_OBJ,US_02_02_POLAR_BASE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Wooden Planks; 2\u00d7 Barrels,260,2150,US_02_02_POLAR_BASE_OBJ_DESC,CONTRACTS\nUS_02_02_SPECIAL_DELIVERY_OBJ,US_02_02_SPECIAL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Service Spare Parts Special,200,1350,US_02_02_SPECIAL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_02_WORKING_STIFF_TSK,US_02_02_WORKING_STIFF_TSK,_TASKS,US_02,Alaska,truckDelivery,,190,2000,US_02_02_WORKING_STIFF_TSK_DESC,TASKS\nUS_02_02_ENVIRONMENTAL_ISSUE_TSK,US_02_02_ENVIRONMENTAL_ISSUE_TSK,_TASKS,US_02,Alaska,truckDelivery,,170,1350,US_02_02_ENVIRONMENTAL_ISSUE_DESC,TASKS\nUS_02_02_SERVICE_CONVOY_TSK,US_02_02_SERVICE_CONVOY_TSK,_TASKS,US_02,Alaska,truckDelivery,,180,1200,US_02_02_SERVICE_CONVOY_TSK_DESC,TASKS\nUS_02_02_BRIDGE_RECOVERY_TSK,US_02_02_BRIDGE_RECOVERY_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Concrete Slab; 2\u00d7 Service Spare Parts,310,2300,US_02_02_BRIDGE_RECOVERY_DESC,TASKS\nUS_02_02_BRICKS_ON_RIVER_TSK,US_02_02_BRICKS_ON_RIVER_TSK,_TASKS,US_02,Alaska,cargoDelivery,3\u00d7 Bricks,210,1800,US_02_02_BRICKS_ON_RIVER_TSK_DESC,TASKS\nUS_02_02_TO_THE_TOWER_CNT,US_02_02_TO_THE_TOWER_CNT,_CONTESTS,US_02,Alaska,exploration,,50,750,US_02_02_TO_THE_TOWER_CNT_DESC,CONTESTS\nUS_02_02_RIVER_CONTEST_CNT,US_02_02_RIVER_CONTEST_CNT,_CONTESTS,US_02,Alaska,exploration,,30,500,US_02_02_RIVER_CONTEST_CNT_DESC,CONTESTS\nUS_02_03_LOGS_01_OBJ,US_02_03_LOGS_01_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Logs Medium; 2\u00d7 Logs Long,1610,18200,US_02_03_LOGS_01_OBJ_DESC,CONTRACTS\nUS_02_03_DISASS_OBJ,US_02_03_DISASS_OBJ,_CONTRACTS,US_02,Alaska,truckDelivery,,190,1750,US_02_03_DISASS_OBJ_DESC,CONTRACTS\nUS_02_03_PIPELINE_OBJ,US_02_03_PIPELINE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts; 1\u00d7 Pipes Medium; 1\u00d7 Wooden Planks; 1\u00d7 Container Large,800,6600,US_02_03_PIPELINE_OBJ_DESC,CONTRACTS\nUS_02_03_TOWN_DELIVERY_OBJ,US_02_03_TOWN_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Blocks; 1\u00d7 Pipes Small; 2\u00d7 Wooden Planks,620,6350,US_02_03_TOWN_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_03_POLAR_BASE_OBJ,US_02_03_POLAR_BASE_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Bags; 2\u00d7 Barrels; 1\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,770,6100,US_02_03_POLAR_BASE_OBJ_DESC,CONTRACTS\nUS_02_03_DRILL_DELIVERY_OBJ,US_02_03_DRILL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Container Large Drilling,790,5900,US_02_03_DRILL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_03_CRATES_OF_CONSUMABLES_OBJ,US_02_03_CRATES_OF_CONSUMABLES_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Crate Large,500,3400,US_02_03_CRATES_OF_CONSUMABLES_OBJ_DESC,CONTRACTS\nUS_02_03_MAZUT_OBJ,US_02_03_MAZUT_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,5\u00d7 Barrels Oil,260,2050,US_02_03_MAZUT_OBJ_DESC,CONTRACTS\nUS_02_03_SPECIAL_DELIVERY_OBJ,US_02_03_SPECIAL_DELIVERY_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,4\u00d7 Service Spare Parts Special,210,1550,US_02_03_SPECIAL_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_03_DERRY_LONGHORN_TSK,US_02_03_DERRY_LONGHORN_TSK,_TASKS,US_02,Alaska,truckDelivery,,210,2050,US_02_03_DERRY_LONGHORN_DESC,TASKS\nUS_02_03_FAILED_FISHING_A_TSK,US_02_03_FAILED_FISHING_A_TSK,_TASKS,US_02,Alaska,truckDelivery,,290,1950,US_02_03_FAILED_FISHING_TSK_DESC,TASKS\nUS_02_03_SCOUT_IN_TROUBLE_TSK,US_02_03_SCOUT_IN_TROUBLE_TSK,_TASKS,US_02,Alaska,truckDelivery,,190,1700,US_02_03_SCOUT_IN_TROUBLE_TSK_DESC,TASKS\nUS_02_03_OUT_OF_FUEL_TSK,US_02_03_OUT_OF_FUEL_TSK,_TASKS,US_02,Alaska,truckDelivery,,160,1500,US_02_03_OUT_OF_FUEL_TSK_DESC,TASKS\nUS_02_03_THEFT_OF_FUEL_TSK,US_02_03_THEFT_OF_FUEL_TSK,_TASKS,US_02,Alaska,truckDelivery,,180,1200,US_02_03_THEFT_OF_FUEL_TSK_DESC,TASKS\nUS_02_03_BLOCKED_TUNNEL_TSK,US_02_03_BLOCKED_TUNNEL_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Pipes Small; 1\u00d7 Blocks,270,2350,US_02_03_BLOCKED_TUNNEL_DESC,TASKS\nUS_02_03_LONG_BRIDGE_RECOVERY_TSK,US_02_03_LONG_BRIDGE_RECOVERY_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Service Spare Parts,220,1500,US_02_03_LONG_BRIDGE_RECOVERY_DESC,TASKS\nUS_02_03_BUILDING_MATERIALS_TSK,US_02_03_BUILDING_MATERIALS_TSK,_TASKS,US_02,Alaska,cargoDelivery,3\u00d7 Bricks,160,1250,US_02_03_BUILDING_MATERIALS_TSK_DESC,TASKS\nUS_02_03_REPAIR_THE_BRIDGE_TSK,US_02_03_REPAIR_THE_BRIDGE_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Service Spare Parts,120,950,US_02_03_REPAIR_THE_BRIDGE_DESC,TASKS\nUS_02_03_WEATHER_FORECAST_CNT,US_02_03_WEATHER_FORECAST_CNT,_CONTESTS,US_02,Alaska,truckDelivery,,300,1950,US_02_03_WEATHER_FORECAST_CNT_DESC,CONTESTS\nUS_02_04_LOGS_01_OBJ,US_02_04_LOGS_01_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Long,1220,14450,US_02_04_LOGS_01_OBJ_DESC,CONTRACTS\nUS_02_04_NEED_MORE_CARGO_ST2_OBJ,US_02_04_NEED_MORE_CARGO_ST2_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,8\u00d7 Bricks; 4\u00d7 Concrete Slab,1850,18850,US_02_04_NEED_MORE_CARGO_ST2_OBJ_DESC,CONTRACTS\nUS_02_04_SPECIAL_CARGO_DELIVERYNG_CNT,US_02_04_SPECIAL_CARGO_DELIVERYNG_CNT,_CONTRACTS,US_02,Alaska,truckDelivery,,180,1500,US_02_04_SPECIAL_CARGO_DELIVERYNG_CNT_DESC,CONTRACTS\nUS_02_04_EPIC_DELIVERY_TIME_OBJ,US_02_04_EPIC_DELIVERY_TIME_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,1\u00d7 Container Large; 1\u00d7 Container Large; 1\u00d7 Concrete Slab; 1\u00d7 Bags; 1\u00d7 Concrete Slab; 1\u00d7 Bags; 1\u00d7 Metal Planks; 2\u00d7 Concrete Slab; 1\u00d7 Container Small; 1\u00d7 Container Small,3170,31700,US_02_04_EPIC_DELIVERY_OBJ_DESC,CONTRACTS\nUS_02_04_NEED_MORE_CARGO_OBJ,US_02_04_NEED_MORE_CARGO_OBJ,_CONTRACTS,US_02,Alaska,cargoDelivery,3\u00d7 Pipe Large; 4\u00d7 Blocks,2380,23600,US_02_04_NEED_MORE_CARGO_OBJ_DESC,CONTRACTS\nUS_02_04_SERVICE_HUB_REACTIVATION_CNT,US_02_04_SERVICE_HUB_REACTIVATION_CNT,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Blocks; 2\u00d7 Crate Large,420,5100,US_02_04_SERVICE_HUB_REACTIVATION_CNT_DESC,CONTRACTS\nUS_02_04_PIPELINE_BUILDING_CNT,US_02_04_PIPELINE_BUILDING_CNT,_CONTRACTS,US_02,Alaska,cargoDelivery,2\u00d7 Metal Planks,310,2450,US_02_04_PIPELINE_BUILDING_CNT_DESC,CONTRACTS\nUS_02_04_SIDEBOARD_SPAWN_TSK,US_02_04_SIDEBOARD_SPAWN_TSK,_TASKS,US_02,Alaska,truckDelivery,,240,2250,US_02_04_SIDEBOARD_SPAWN_TSK_DESC,TASKS\nUS_02_04_CAR_HELP_TSK,US_02_04_CAR_HELP_TSK,_TASKS,US_02,Alaska,truckDelivery,,190,1650,US_02_04_CAR_HELP_TSK_DESC,TASKS\nUS_02_04_MOUNTAIN_CLEANING_TSK,US_02_04_MOUNTAIN_CLEANING_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Wooden Planks,230,2550,US_02_04_MOUNTAIN_CLEARING_TSK_DESC,TASKS\nUS_02_04_LOST_CARGO_TSK,US_02_04_LOST_CARGO_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Vehicles Spare Parts,150,1550,US_02_04_LOST_CARGO_TSK_DESC,TASKS\nUS_02_04_MATERIAL_DELIVERYING_TSK,US_02_04_MATERIAL_DELIVERYING_TSK,_TASKS,US_02,Alaska,cargoDelivery,1\u00d7 Barrels Oil; 2\u00d7 Wooden Planks,150,1350,US_02_04_MATERIAL_DELIVERYING_TSK_DESC,TASKS\nUS_02_04_BROKEN_POLE_TSK,US_02_04_BROKEN_POLE_TSK,_TASKS,US_02,Alaska,cargoDelivery,2\u00d7 Wooden Planks,130,1250,US_02_04_BROKEN_POLE_TSK_DESC,TASKS\nUS_02_04_BRIDGE_BUILDING_TSK,US_02_04_BRIDGE_BUILDING_TSK,_TASKS,US_02,Alaska,cargoDelivery,4\u00d7 Wooden Planks,140,1100,US_02_04_BRIDGE_BUILDING_TSK_DESC,TASKS\nUS_02_04_FARMER_HOME_TSK,US_02_04_FARMER_HOME_TSK,_TASKS,US_02,Alaska,exploration,,80,1300,US_02_04_FARMER_HOME_TSK_DESC,TASKS\nUS_02_04_FRAGILE_DELIVERY_CNT,US_02_04_FRAGILE_DELIVERY_CNT,_CONTESTS,US_02,Alaska,truckDelivery,,140,1300,US_02_04_FRAGILE_DELIVERY_CNT_DESC,CONTESTS\nRU_02_01_SERVICE_HUB_RECOVERY_01_OBJ,RU_02_01_SERVICE_HUB_RECOVERY_01_OBJ,_CONTRACTS,RU_02,Taymyr,truckDelivery,,230,2050,RU_02_01_SERVICE_HUB_RECOVERY_01,CONTRACTS\nRU_02_01_PROSPECTING_01_OBJ,RU_02_01_PROSPECTING_01_OBJ,_CONTRACTS,RU_02,Taymyr,exploration,,740,4600,RU_02_01_PROSPECTING_01,CONTRACTS\nRU_02_01_SERVICE_HUB_RECOVERY_02_OBJ,RU_02_01_SERVICE_HUB_RECOVERY_02_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Vehicles Spare Parts,340,3800,RU_02_01_SERVICE_HUB_RECOVERY_02,CONTRACTS\nRU_02_01_PROSPECTING_02_OBJ,RU_02_01_PROSPECTING_02_OBJ,_CONTRACTS,RU_02,Taymyr,exploration,,1510,9950,RU_02_01_PROSPECTING_02,CONTRACTS\nRU_02_01_LOG_SHIPWRECK_SUPPLY_OBJ,RU_02_01_LOG_SHIPWRECK_SUPPLY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Logs Medium,670,5950,RU_02_01_LOG_SHIPWRECK_SUPPLY_DESC,CONTRACTS\nRU_02_01_LOG_QUARRY_REINFORCEMENT_OBJ,RU_02_01_LOG_QUARRY_REINFORCEMENT_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Logs Medium,630,5350,RU_02_01_LOG_QUARRY_REINFORCEMENT_DESC,CONTRACTS\nRU_02_01_OILRIG_RECOVERY_OBJ,RU_02_01_OILRIG_RECOVERY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,1\u00d7 Container Large,580,4850,RU_02_01_OILRIG_RECOVERY,CONTRACTS\nRU_02_01_REFUEL_TRUCK_SWAMP_TSK,RU_02_01_REFUEL_TRUCK_SWAMP_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,450,5950,RU_02_01_REFUEL_TRUCK_SWAMP_TSK_0_DESC,TASKS\nRU_02_01_REPAIR_TRUCK_HIGHWAY_TSK,RU_02_01_REPAIR_TRUCK_HIGHWAY_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,430,5550,RU_02_01_REPAIR_TRUCK_HIGHWAY_TSK_0_DESC,TASKS\nRU_02_01_HERMIT_RESCUE_TSK,RU_02_01_HERMIT_RESCUE_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,350,4250,RU_02_01_HERMIT_RESCUE_TSK_DESC,TASKS\nRU_02_01_OILRIG_SAMPLING_TSK,RU_02_01_OILRIG_SAMPLING_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,300,4050,RU_02_01_OILRIG_SAMPLING_TSK_0_DESC,TASKS\nRU_02_01_EXAMINE_SOUTH_TSK,RU_02_01_EXAMINE_SOUTH_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,200,1500,RU_02_01_EXAMINE_SOUTH_TSK_0_DESC,TASKS\nRU_02_01_TOWER_CLEARING_A_TSK,RU_02_01_TOWER_CLEARING_A_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,2\u00d7 Metal Planks,600,5500,RU_02_01_TOWER_CLEARING_A,TASKS\nRU_02_01_VILLAGE_RESTORATION_TSK,RU_02_01_VILLAGE_RESTORATION_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,3\u00d7 Wooden Planks,160,1400,RU_02_01_VILLAGE_RESTORATION_TSK_DESC,TASKS\nRU_02_01_HTRUCK_REFUEL_TSK,RU_02_01_HTRUCK_REFUEL_TSK,_TASKS,RU_02,Taymyr,exploration,,330,3900,RU_02_01_HTRUCK_REFUEL,TASKS\nRU_02_01_EXAMINE_EAST_TSK,RU_02_01_EXAMINE_EAST_TSK,_TASKS,RU_02,Taymyr,exploration,,70,1200,RU_02_01_EXAMINE_EAST_TSK_DESC,TASKS\nRU_02_01_FIREWATCH_SUPPLY_CNT,RU_02_01_FIREWATCH_SUPPLY_CNT,_CONTESTS,RU_02,Taymyr,truckDelivery,,120,950,RU_02_01_FIREWATCH_SUPPLY_DESC,CONTESTS\nRU_02_01_SERVHUB_FUEL_RESTOCK_CNT,RU_02_01_SERVHUB_FUEL_RESTOCK_CNT,_CONTESTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Barrels,200,1650,RU_02_01_SERVHUB_FUEL_RESTOCK_DESC,CONTESTS\nRU_02_01_SHIP_REPAIRS_CNT,RU_02_01_SHIP_REPAIRS_CNT,_CONTESTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Service Spare Parts,130,1050,RU_02_01_SHIP_REPAIRS_CNT_DESC,CONTESTS\nRU_02_02_LOST_CARGO_01_TSK,RU_02_02_LOST_CARGO_01_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,220,2300,RU_02_02_LOST_CARGO_01_DESC,TASKS\nRU_02_02_DAMAGED_TRUCK_01_TSK,RU_02_02_DAMAGED_TRUCK_01_TSK,_TASKS,RU_02,Taymyr,exploration,,290,3150,RU_02_02_DAMAGED_TRUCK_01_TSK_DESC,TASKS\nRU_02_02_FLAG_1_CNT,RU_02_02_FLAG_1_CNT,_CONTESTS,RU_02,Taymyr,exploration,,60,900,RU_02_02_FLAG_1_START_CNT_DESC,CONTESTS\nRU_02_02_HUB_RECOVERY_2,RU_02_02_HUB_RECOVERY_2,_CONTRACTS,RU_02,Taymyr,cargoDelivery,4\u00d7 Vehicles Spare Parts,310,2400,RU_02_02_HUB_RECOVERY_2_DESC,CONTRACTS\nRU_02_02_STUCK_TRUCK_02_TSK,RU_02_02_STUCK_TRUCK_02_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,240,2350,RU_02_02_STUCK_TRUCK_02_TSK_DESC,TASKS\nRU_02_02_LOST_CARGO_02_TSK,RU_02_02_LOST_CARGO_02_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,1\u00d7 Bricks,200,2250,RU_02_02_LOST_CARGO_02_DESC,TASKS\nRU_02_02_DAMAGED_TRUCK_02_TSK,RU_02_02_DAMAGED_TRUCK_02_TSK,_TASKS,RU_02,Taymyr,exploration,,230,2450,RU_02_02_DAMAGED_TRUCK_02_TSK_DESC,TASKS\nRU_02_02_FLAG_2_CNT,RU_02_02_FLAG_2_CNT,_CONTESTS,RU_02,Taymyr,exploration,,60,850,RU_02_02_FLAG_2_CNT_DESC,CONTESTS\nRU_02_02_STUCK_TRUCK_03_TSK,RU_02_02_STUCK_TRUCK_03_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,290,2300,RU_02_02_STUCK_TRUCK_03_TSK_DESC,TASKS\nRU_02_02_LOST_CARGO_03_TSK,RU_02_02_LOST_CARGO_03_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,190,1450,RU_02_02_LOST_CARGO_03_TSK_DESC,TASKS\nRU_02_02_DAMAGED_TRUCK_03_TSK,RU_02_02_DAMAGED_TRUCK_03_TSK,_TASKS,RU_02,Taymyr,exploration,,230,2050,RU_02_02_DAMAGED_TRUCK_03_TSK_DESC,TASKS\nRU_02_02_STUCK_TRUCK_04_TSK,RU_02_02_STUCK_TRUCK_04_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,260,2650,RU_02_02_STUCK_TRUCK_04_TSK_DESC,TASKS\nRU_02_02_LOST_CARGO_04_TSK,RU_02_02_LOST_CARGO_04_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,1\u00d7 Bricks,190,1600,RU_02_02_LOST_CARGO_04_DESC,TASKS\nRU_02_02_DAMAGED_TRUCK_04_TSK,RU_02_02_DAMAGED_TRUCK_04_TSK,_TASKS,RU_02,Taymyr,exploration,,190,1450,RU_02_02_DAMAGED_TRUCK_04_TSK_DESC,TASKS\nRU_02_02_STUCK_TRUCK_05_TSK,RU_02_02_STUCK_TRUCK_05_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,180,1200,RU_02_02_STUCK_TRUCK_05_TSK_DESC,TASKS\nRU_02_02_LOST_CARGO_05_TSK,RU_02_02_LOST_CARGO_05_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,1\u00d7 Bricks,200,1250,RU_02_02_LOST_CARGO_05_DESC,TASKS\nRU_02_02_HUB_RECOVERY,RU_02_02_HUB_RECOVERY,_CONTRACTS,RU_02,Taymyr,truckDelivery,,290,2700,RU_02_02_HUB_RECOVERY_DESC,CONTRACTS\nRU_02_02_LOG_FARM_SUPPLY_OBJ,RU_02_02_LOG_FARM_SUPPLY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,6\u00d7 Logs Medium,1470,16800,RU_02_02_LOG_FARM_SUPPLY_DESC,CONTRACTS\nRU_02_02_LOG_LUMBERMILL_RESTOCK_OBJ,RU_02_02_LOG_LUMBERMILL_RESTOCK_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Logs Long,1170,10150,RU_02_02_LOG_LUMBERMILL_RESTOCK_DESC,CONTRACTS\nRU_02_02_GORLAG_CLEANING,RU_02_02_GORLAG_CLEANING,_CONTRACTS,RU_02,Taymyr,cargoDelivery,4\u00d7 Radioctive; 2\u00d7 Radioctive,730,6900,RU_02_02_GORLAG_CLEANING_DESC,CONTRACTS\nRU_02_02_RADAR_TOWER_RECOVERY,RU_02_02_RADAR_TOWER_RECOVERY,_CONTRACTS,RU_02,Taymyr,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Service Spare Parts; 1\u00d7 Container Small,710,6150,RU_02_02_RADAR_TOWER_RECOVERY_DESC,CONTRACTS\nRU_02_02_MAZUT_DELIVERY,RU_02_02_MAZUT_DELIVERY,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil,560,5750,RU_02_02_MAZUT_DELIVERY_DESC,CONTRACTS\nRU_02_02_FARM_SUPPLY,RU_02_02_FARM_SUPPLY,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Bags; 2\u00d7 Wooden Planks; 1\u00d7 Concrete Slab,510,4800,RU_02_02_FARM_SUPPLY_DESC,CONTRACTS\nRU_02_02_WOODEN_PLANKS_DELIVERY,RU_02_02_WOODEN_PLANKS_DELIVERY,_CONTRACTS,RU_02,Taymyr,cargoDelivery,5\u00d7 Wooden Planks,260,2750,RU_02_02_WOODEN_PLANKS_DELIVERY_DESC,CONTRACTS\nRU_02_02_RESEARCH,RU_02_02_RESEARCH,_CONTRACTS,RU_02,Taymyr,exploration,,570,9950,RU_02_02_RESEARCH_DESC,CONTRACTS\nRU_02_02_STUCK_TRUCK_TSK,RU_02_02_STUCK_TRUCK_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,240,2350,RU_02_02_STUCK_TRUCK_TSK_DESC,TASKS\nRU_02_02_CONTAINER_DELIVERY_CNT,RU_02_02_CONTAINER_DELIVERY_CNT,_CONTESTS,RU_02,Taymyr,cargoDelivery,1\u00d7 Container Small,150,1600,RU_02_02_CONTAINER_DELIVERY_CNT_DESC,CONTESTS\nRU_02_02_BARRELS_DELIVERY_CNT,RU_02_02_BARRELS_DELIVERY_CNT,_CONTESTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Barrels,150,1550,RU_02_02_BARRELS_DELIVERY_CNT_DESC,CONTESTS\nRU_02_03_LOG_SAWMILL_RESTOCK_OBJ,RU_02_03_LOG_SAWMILL_RESTOCK_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,3\u00d7 Logs Long,1530,17700,RU_02_03_LOG_SAWMILL_RESTOCK_DESC,CONTRACTS\nRU_02_03_UPD_PORTSIDE_RESUPPLY_OBJ,RU_02_03_UPD_PORTSIDE_RESUPPLY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Concrete Slab; 4\u00d7 Vehicles Spare Parts,1320,12200,RU_02_03_UPD_PORTSIDE_RESUPPLY_DESC,CONTRACTS\nRU_02_03_DERRICK_DELIVERY_OBJ,RU_02_03_DERRICK_DELIVERY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,1\u00d7 Big Drills,1080,8900,RU_02_03_DERRICK_DELIVERY_DESC,CONTRACTS\nRU_02_03_PIER_RECOVERY_OBJ,RU_02_03_PIER_RECOVERY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Concrete Slab; 1\u00d7 Container Large,1090,8250,RU_02_03_PIER_RECOVERY_DESC,CONTRACTS\nRU_02_03_DRILLING_EQUIPMENT_DELIVERY_OBJ,RU_02_03_DRILLING_EQUIPMENT_DELIVERY_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,1\u00d7 Container Large; 2\u00d7 Container Small,1050,7550,RU_02_03_DRILLING_EQUIPMENT_DELIVERY_DESC,CONTRACTS\nRU_02_03_GARAGE_AND_WAREHOUSE_RESTORATION_OBJ,RU_02_03_GARAGE_AND_WAREHOUSE_RESTORATION_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Concrete Slab; 1\u00d7 Barrels,840,7500,RU_02_03_GARAGE_AND_WAREHOUSE_RESTORATION_DESC,CONTRACTS\nRU_02_03_CONTRACT_SCAN_POINTS_OBJ,RU_02_03_CONTRACT_SCAN_POINTS_OBJ,_CONTRACTS,RU_02,Taymyr,exploration,,260,4550,RU_02_03_CONTRACT_SCAN_POINTS_DESC,CONTRACTS\nRU_02_03_TASK_FIND_THE_CAR_OBJ,RU_02_03_TASK_FIND_THE_CAR_OBJ,_TASKS,RU_02,Taymyr,truckDelivery,,200,1600,RU_02_03_TASK_FIND_THE_CAR_DESC,TASKS\nRU_02_03_TASK_SEARCH_OBJ,RU_02_03_TASK_SEARCH_OBJ,_TASKS,RU_02,Taymyr,cargoDelivery,4\u00d7 Crate Large,930,6350,RU_02_03_TASK_SEARCH_DESC,TASKS\nRU_02_03_SAWMILL_RECOVERY_OBJ,RU_02_03_SAWMILL_RECOVERY_OBJ,_TASKS,RU_02,Taymyr,cargoDelivery,1\u00d7 Container Large; 2\u00d7 Metal Planks,850,5450,RU_02_03_SAWMILL_RECOVERY_DESC,TASKS\nRU_02_03_TASK_BUILD_BRIDGE_OBJ,RU_02_03_TASK_BUILD_BRIDGE_OBJ,_TASKS,RU_02,Taymyr,cargoDelivery,2\u00d7 Concrete Slab,530,3950,RU_02_03_TASK_BUILD_BRIDGE_DESC,TASKS\nRU_02_03_TASK_METAL_DELIVERY_OBJ,RU_02_03_TASK_METAL_DELIVERY_OBJ,_TASKS,RU_02,Taymyr,cargoDelivery,2\u00d7 Metal Planks,390,2300,RU_02_03_TASK_METAL_DELIVERY_DESC,TASKS\nRU_02_03_TASK_FIND_THE_TRUCK_OBJ,RU_02_03_TASK_FIND_THE_TRUCK_OBJ,_TASKS,RU_02,Taymyr,exploration,,370,4550,RU_02_03_TASK_FIND_THE_TRUCK_DESC,TASKS\nRU_02_03_TASK_DOCUMENTARY_OBJ,RU_02_03_TASK_DOCUMENTARY_OBJ,_TASKS,RU_02,Taymyr,exploration,,50,850,RU_02_03_TASK_DOCUMENTARY_DESC,TASKS\nRU_02_03_CONTEST_METAL_DELIVERY_OBJ,RU_02_03_CONTEST_METAL_DELIVERY_OBJ,_CONTESTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Metal Planks,300,2100,RU_02_03_CONTEST_METAL_DELIVERY_DESC,CONTESTS\nRU_02_03_CONTEST_BARRELS_DELIVERY_OBJ,RU_02_03_CONTEST_BARRELS_DELIVERY_OBJ,_CONTESTS,RU_02,Taymyr,cargoDelivery,4\u00d7 Bags,280,1750,RU_02_03_CONTEST_BARRELS_DELIVERY_DESC,CONTESTS\nRU_02_03_CONTEST_WOODEN_DELIVEY_PIRS_OBJ,RU_02_03_CONTEST_WOODEN_DELIVEY_PIRS_OBJ,_CONTESTS,RU_02,Taymyr,cargoDelivery,4\u00d7 Wooden Planks,110,1050,RU_02_03_CONTEST_WOODEN_DELIVEY_DESC,CONTESTS\nRU_02_03_CONTEST_WOODEN_DELIVEY_WAREHOUSE_OBJ,RU_02_03_CONTEST_WOODEN_DELIVEY_WAREHOUSE_OBJ,_CONTESTS,RU_02,Taymyr,cargoDelivery,4\u00d7 Wooden Planks,100,950,RU_02_03_CONTEST_WOODEN_DELIVEY_WAREHOUSE_DESC,CONTESTS\nRU_02_04_MINES_EXPLORATION_OBJ,RU_02_04_MINES_EXPLORATION_OBJ,_CONTRACTS,RU_02,Taymyr,truckDelivery,,340,3300,RU_02_04_MINES_EXPLORATION_01,CONTRACTS\nRU_02_04_UPD_VILLAGE_RESTOCK_OBJ,RU_02_04_UPD_VILLAGE_RESTOCK_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Vehicles Spare Parts,610,7950,RU_02_04_UPD_VILLAGE_RESTOCK_DESC,CONTRACTS\nRU_02_04_MAST_RESTORATION_OBJ,RU_02_04_MAST_RESTORATION_OBJ,_CONTRACTS,RU_02,Taymyr,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Metal Planks,960,7750,RU_02_04_MAST_RESTORATION_01,CONTRACTS\nRU_02_04_NORTH_ROAD_EXPLORATION_OBJ,RU_02_04_NORTH_ROAD_EXPLORATION_OBJ,_CONTRACTS,RU_02,Taymyr,exploration,,80,1300,RU_02_04_NORTH_ROAD_EXPLORATION_01,CONTRACTS\nRU_02_04_RECOVER_LOAF_TSK,RU_02_04_RECOVER_LOAF_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,590,5050,RU_02_04_RECOVER_LOAF_01,TASKS\nRU_02_04_REFUEL_TRUCK_TSK,RU_02_04_REFUEL_TRUCK_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,520,4450,RU_02_04_REFUEL_TRUCK,TASKS\nRU_02_04_MAST_FUELSTATION_TSK,RU_02_04_MAST_FUELSTATION_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,360,3550,RU_02_04_MAST_FUELSTATION_01,TASKS\nRU_02_04_RECOVER_APC_TSK,RU_02_04_RECOVER_APC_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,470,2950,RU_02_04_RECOVER_APC_01,TASKS\nRU_02_04_DELIVER_SUPPLY_TRAILER_TSK,RU_02_04_DELIVER_SUPPLY_TRAILER_TSK,_TASKS,RU_02,Taymyr,truckDelivery,,320,2850,RU_02_04_DELIVER_SUPPLY_TRAILER_01,TASKS\nRU_02_04_RESUPPLY_TSK,RU_02_04_RESUPPLY_TSK,_TASKS,RU_02,Taymyr,cargoDelivery,1\u00d7 Bags; 1\u00d7 Bags; 1\u00d7 Bags; 1\u00d7 Bags,1260,7100,RU_02_04_RESUPPLY_01,TASKS\nRU_02_04_REPAIR_ZIKZ_TSK,RU_02_04_REPAIR_ZIKZ_TSK,_TASKS,RU_02,Taymyr,exploration,,200,1550,RU_02_04_REPAIR_ZIKZ_01,TASKS\nRU_02_04_EXAMINE_CAMP_TSK,RU_02_04_EXAMINE_CAMP_TSK,_TASKS,RU_02,Taymyr,exploration,,90,1550,RU_02_04_EXAMINE_CAMP_01,TASKS\nRU_02_04_RIFT_MAPPING_CNT,RU_02_04_RIFT_MAPPING_CNT,_CONTESTS,RU_02,Taymyr,exploration,,50,850,RU_02_04_RIFT_MAPPING_DESC,CONTESTS\nRU_03_01_POWERLINE_1_REPAIR_TSK,RU_03_01_POWERLINE_1_REPAIR_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Metal Planks,460,3700,RU_03_01_POWERLINE_1_REPAIR_DESC,TASKS\nRU_03_01_POWERLINE_3_REPAIR_TSK,RU_03_01_POWERLINE_3_REPAIR_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Service Spare Parts,280,3000,RU_03_01_POWERLINE_3_REPAIR_DESC,TASKS\nRU_03_01_CAMP_SETUP_OBJ,RU_03_01_CAMP_SETUP_OBJ,_CONTRACTS,RU_03,Kola Peninsula,truckDelivery,,610,6100,RU_03_01_CAMP_SETUP_DESC,CONTRACTS\nRU_03_01_MOBILE_WATCHTOWER_RECOVERY_OBJ,RU_03_01_MOBILE_WATCHTOWER_RECOVERY_OBJ,_CONTRACTS,RU_03,Kola Peninsula,truckDelivery,,450,4750,RU_03_01_MOBILE_WATCHTOWER_RECOVERY_DESC,CONTRACTS\nRU_03_01_LOG_VILLAGE_DELIVERY_OBJ,RU_03_01_LOG_VILLAGE_DELIVERY_OBJ,_CONTRACTS,RU_03,Kola Peninsula,cargoDelivery,3\u00d7 Logs Medium,960,10250,RU_03_01_LOG_VILLAGE_DELIVERY_DESC,CONTRACTS\nRU_03_01_BUNKER_OBJ,RU_03_01_BUNKER_OBJ,_CONTRACTS,RU_03,Kola Peninsula,exploration,,340,5800,RU_03_01_BUNKER_DESC,CONTRACTS\nRU_03_01_POWERLINE_INSPECTION_OBJ,RU_03_01_POWERLINE_INSPECTION_OBJ,_CONTRACTS,RU_03,Kola Peninsula,exploration,,230,3950,RU_03_01_POWERLINE_INSPECTION_DESC,CONTRACTS\nRU_03_01_SWAMP_ROUTE_CHECK_OBJ,RU_03_01_SWAMP_ROUTE_CHECK_OBJ,_CONTRACTS,RU_03,Kola Peninsula,exploration,,210,3550,RU_03_01_SWAMP_ROUTE_CHECK_DESC,CONTRACTS\nRU_03_01_NORTH_ROUTE_CHECK_OBJ,RU_03_01_NORTH_ROUTE_CHECK_OBJ,_CONTRACTS,RU_03,Kola Peninsula,exploration,,190,3200,RU_03_01_NORTH_ROUTE_CHECK_DESC,CONTRACTS\nRU_03_01_ROUTE_CHECK_OBJ,RU_03_01_ROUTE_CHECK_OBJ,_CONTRACTS,RU_03,Kola Peninsula,exploration,,150,2550,RU_03_01_ROUTE_CHECK_DESC,CONTRACTS\nRU_03_01_SNOWED_IN_TSK,RU_03_01_SNOWED_IN_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,680,7300,RU_03_01_SNOWED_IN_DESC,TASKS\nRU_03_01_ZIKZ_RECOVERY_TSK,RU_03_01_ZIKZ_RECOVERY_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,610,6100,RU_03_01_ZIKZ_RECOVERY_DESC,TASKS\nRU_03_01_CAMP_GAS_DELIVEY_TSK,RU_03_01_CAMP_GAS_DELIVEY_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,410,4450,RU_03_01_CAMP_GAS_DELIVERY_DESC,TASKS\nRU_03_01_FOREIGNER_TSK,RU_03_01_FOREIGNER_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,410,4100,RU_03_01_FOREIGNER_DESC,TASKS\nRU_03_01_ANTIQUE_TRUCK_HUNT_TSK,RU_03_01_ANTIQUE_TRUCK_HUNT_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,350,3050,RU_03_01_ANTIQUE_TRUCK_HUNT_DESC,TASKS\nRU_03_01_ON_THIN_ICE_TSK,RU_03_01_ON_THIN_ICE_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,300,2550,RU_03_01_ON_THIN_ICE_DESC,TASKS\nRU_03_01_CAMP_RESUPPLY_TSK,RU_03_01_CAMP_RESUPPLY_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts,650,8350,RU_03_01_CAMP_RESUPPLY_DESC,TASKS\nRU_03_01_BASE_SUPPLIES_TSK,RU_03_01_BASE_SUPPLIES_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Radioctive,450,6250,RU_03_01_BASE_SUPPLIES_DESC,TASKS\nRU_03_01_BRIDGE_REPAIR_B_TSK,RU_03_01_BRIDGE_REPAIR_B_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Metal Planks,460,4850,RU_03_01_BRIDGE_REPAIR_B_DESC,TASKS\nRU_03_01_MILITARY_SUPPLY_TSK,RU_03_01_MILITARY_SUPPLY_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Barrels Oil,390,4750,RU_03_01_MILITARY_SUPPLY_DESC,TASKS\nRU_03_01_HELI_CARGO_RECOVERY_TSK,RU_03_01_HELI_CARGO_RECOVERY_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Vehicles Spare Parts,370,3800,RU_03_01_HELI_CARGO_RECOVERY_DESC,TASKS\nRU_03_01_METEO_RESTORE_TSK,RU_03_01_METEO_RESTORE_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Wooden Planks,250,3250,RU_03_01_METEO_RESTORE_DESC,TASKS\nRU_03_01_LOG_CABIN_REPAIR_TSK,RU_03_01_LOG_CABIN_REPAIR_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Wooden Planks,210,2500,RU_03_01_LOG_CABIN_REPAIR_DESC,TASKS\nRU_03_01_RURAL_SPELUNKER_TSK,RU_03_01_RURAL_SPELUNKER_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,560,9800,RU_03_01_RURAL_SPELUNKER_DESC,TASKS\nRU_03_01_FACTORY_CHECK_TSK,RU_03_01_FACTORY_CHECK_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,210,3650,RU_03_01_FACTORY_CHECK_DESC,TASKS\nRU_03_01_METEO_CHECK_TSK,RU_03_01_METEO_CHECK_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,180,3150,RU_03_01_METEO_CHECK_DESC,TASKS\nRU_03_01_CAMP_ROUTE_CHECK_TSK,RU_03_01_CAMP_ROUTE_CHECK_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,170,3000,RU_03_01_CAMP_ROUTE_CHECK_DESC,TASKS\nRU_03_01_VILLAGE_SCOUTING_TSK,RU_03_01_VILLAGE_SCOUTING_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,130,2150,RU_03_01_VILLAGE_SCOUTING_DESC,TASKS\nRU_03_01_NORTH_RIVER_RACE_CNT,RU_03_01_NORTH_RIVER_RACE_CNT,_CONTESTS,RU_03,Kola Peninsula,exploration,,70,1250,RU_03_01_NORTH_RIVER_RACE_DESC,CONTESTS\nRU_03_01_OFF_THE_RAILS_CNT,RU_03_01_OFF_THE_RAILS_CNT,_CONTESTS,RU_03,Kola Peninsula,exploration,,70,1150,RU_03_01_OFF_THE_RAILS_DESC,CONTESTS\nRU_03_02_LANDSLIDE_1_TSK,RU_03_02_LANDSLIDE_1_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,2\u00d7 Barrels,390,3750,RU_03_02_LANDSLIDE_1_TSK_DESC,TASKS\nRU_03_02_CAMPUS_1_TSK,RU_03_02_CAMPUS_1_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,250,4350,RU_03_02_CAMPUS_1_TSK_DESC,TASKS\nRU_03_02_LANDSLIDE_2_TSK,RU_03_02_LANDSLIDE_2_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Container Small,490,5600,RU_03_02_LANDSLIDE_2_TSK_DESC,TASKS\nRU_03_02_AIRPLANE_CNT,RU_03_02_AIRPLANE_CNT,_CONTRACTS,RU_03,Kola Peninsula,truckDelivery,1\u00d7 Big Wing; 1\u00d7 Plane; 1\u00d7 Wing,1910,18650,RU_03_02_AIRPLANE_CNT_DESC,CONTRACTS\nRU_03_02_LOG_WAREHOUSE_SUPPLY_OBJ,RU_03_02_LOG_WAREHOUSE_SUPPLY_OBJ,_CONTRACTS,RU_03,Kola Peninsula,cargoDelivery,3\u00d7 Logs Long,1470,16800,RU_03_02_LOG_WAREHOUSE_SUPPLY_DESC,CONTRACTS\nRU_03_02_TANK_CNT,RU_03_02_TANK_CNT,_CONTRACTS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Ba20; 1\u00d7 Ba20,1600,15350,RU_03_02_TANK_CNT_DESC,CONTRACTS\nRU_03_02_LOG_CAMP_DELIVERY_OBJ,RU_03_02_LOG_CAMP_DELIVERY_OBJ,_CONTRACTS,RU_03,Kola Peninsula,cargoDelivery,3\u00d7 Logs Medium,980,11400,RU_03_02_LOG_CAMP_DELIVERY_DESC,CONTRACTS\nRU_03_02_ARCH_CNT,RU_03_02_ARCH_CNT,_CONTRACTS,RU_03,Kola Peninsula,exploration,,540,9450,RU_03_02_ARCH_CNT_DESC,CONTRACTS\nRU_03_02_BUNKER_CNT,RU_03_02_BUNKER_CNT,_CONTRACTS,RU_03,Kola Peninsula,exploration,,410,7150,RU_03_02_BUNKER_CNT_DESC,CONTRACTS\nRU_03_02_ANTIQUES_TSK,RU_03_02_ANTIQUES_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,520,6450,RU_03_02_ANTIQUES_TSK_DESC,TASKS\nRU_03_02_NEW_FISHING_TSK,RU_03_02_NEW_FISHING_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,400,3900,RU_03_02_NEW_FISHING_TSK_DESC,TASKS\nRU_03_02_GARAGE_REPAIR_TSK,RU_03_02_GARAGE_REPAIR_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,1\u00d7 Vehicles Spare Parts,440,3350,RU_03_02_GARAGE_REPAIR_TSK_DESC,TASKS\nRU_03_02_FISHERCAMP_TSK,RU_03_02_FISHERCAMP_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,350,2700,RU_03_02_FISHERCAMP_TSK_DESC,TASKS\nRU_03_02_CAR_DELIVERY_TSK,RU_03_02_CAR_DELIVERY_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,320,2550,RU_03_02_CAR_DELIVERY_TSK_DESC,TASKS\nRU_03_02_LOST_CAR_TSK,RU_03_02_LOST_CAR_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,320,2500,RU_03_02_LOST_CAR_TSK_DESC,TASKS\nRU_03_02_TRAILER_CARGO_DELIVERY_TSK,RU_03_02_TRAILER_CARGO_DELIVERY_TSK,_TASKS,RU_03,Kola Peninsula,truckDelivery,,280,1900,RU_03_02_TRAILER_CARGO_DELIVERY_TSK_DESC,TASKS\nRU_03_02_SUPPLIES_TSK,RU_03_02_SUPPLIES_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Container Small,510,5950,RU_03_02_SUPPLIES_TSK_DESC,TASKS\nRU_03_02_WAREHOUSE_TSK,RU_03_02_WAREHOUSE_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,2\u00d7 Metal Planks,700,4900,RU_03_02_WAREHOUSE_TSK_DESC,TASKS\nRU_03_02_POWER_PLANTS_TSK,RU_03_02_POWER_PLANTS_TSK,_TASKS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Wooden Planks; 1\u00d7 Wooden Planks,490,4300,RU_03_02_POWER_PLANTS_TSK_DESC,TASKS\nRU_03_02_METEO_TSK,RU_03_02_METEO_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,650,11300,RU_03_02_METEO_TSK_DESC,TASKS\nRU_03_02_VILLAGE_TSK,RU_03_02_VILLAGE_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,300,5250,RU_03_02_VILLAGE_TSK_DESC,TASKS\nRU_03_02_ICE_ROAD_TSK,RU_03_02_ICE_ROAD_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,280,4850,RU_03_02_ICE_ROAD_TSK_DESC,TASKS\nRU_03_02_OLD_SHIP_TSK,RU_03_02_OLD_SHIP_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,280,4800,RU_03_02_OLD_SHIP_TSK_DESC,TASKS\nRU_03_02_LIGHTHOUSE_TSK,RU_03_02_LIGHTHOUSE_TSK,_TASKS,RU_03,Kola Peninsula,exploration,,210,3550,RU_03_02_LIGHTHOUSE_TSK_DESC,TASKS\nRU_03_02_GARBAGE_CNT,RU_03_02_GARBAGE_CNT,_CONTESTS,RU_03,Kola Peninsula,cargoDelivery,1\u00d7 Metal Planks,220,1600,RU_03_02_GARBAGE_CNT_DESC,CONTESTS\nUS_04_01_CONV_01_CONT,US_04_01_CONV_01_CONT,_CONTRACTS,US_04,Yukon,cargoDelivery,4\u00d7 Crate Large; 2\u00d7 Barrels; 2\u00d7 Metal Planks; 1\u00d7 Forklift Caravan Container 2,1630,11000,US_04_01_CONV_01_CONT_DESC,CONTRACTS\nUS_04_01_TSK_01,US_04_01_TSK_01,_TASKS,US_04,Yukon,truckDelivery,,460,4600,US_04_01_TSK_01_DESC,TASKS\nUS_04_01_TSK_BRIDGE_01,US_04_01_TSK_BRIDGE_01,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Metal Planks,650,4100,US_04_01_TSK_BRIDGE_01_DESC,TASKS\nUS_04_01_TSK_ROCKS_01,US_04_01_TSK_ROCKS_01,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Crate Large,450,3550,US_04_01_TSK_ROCKS_01_DESC,TASKS\nUS_04_01_DOWNHILL_01_CNT,US_04_01_DOWNHILL_01_CNT,_CONTESTS,US_04,Yukon,exploration,,40,700,US_04_01_DOWNHILL_01_CNT_DESC,CONTESTS\nUS_04_01_CONV_02_CONT,US_04_01_CONV_02_CONT,_CONTRACTS,US_04,Yukon,cargoDelivery,2\u00d7 Blocks; 4\u00d7 Pipes Small; 4\u00d7 Wooden Planks,1510,14650,US_04_01_CONV_02_CONT_DESC,CONTRACTS\nUS_04_01_TSK_02,US_04_01_TSK_02,_TASKS,US_04,Yukon,truckDelivery,,360,3300,US_04_01_TSK_02_DESC,TASKS\nUS_04_01_TSK_BRIDGE_02,US_04_01_TSK_BRIDGE_02,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Wooden Planks,380,4600,US_04_01_TSK_BRIDGE_02_DESC,TASKS\nUS_04_01_CONV_03_CONT,US_04_01_CONV_03_CONT,_CONTRACTS,US_04,Yukon,cargoDelivery,4\u00d7 Metal Roll; 4\u00d7 Bricks; 1\u00d7 Concrete Slab; 1\u00d7 Forklift Caravan Container 2,1100,8300,US_04_01_CONV_03_CONT_DESC,CONTRACTS\nUS_04_01_TSK_BRIDGE_03,US_04_01_TSK_BRIDGE_03,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Metal Planks,660,5250,US_04_01_TSK_BRIDGE_03_DESC,TASKS\nUS_04_01_TSK_03,US_04_01_TSK_03,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Service Spare Parts,230,1850,US_04_01_TSK_03_DESC,TASKS\nUS_04_01_TSK_04,US_04_01_TSK_04,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Service Spare Parts,300,2600,US_04_01_TSK_04_DESC,TASKS\nUS_04_01_TSK_05,US_04_01_TSK_05,_TASKS,US_04,Yukon,truckDelivery,1\u00d7 Barrels,780,8550,US_04_01_TSK_05_DESC,TASKS\nUS_04_01_TSK_06,US_04_01_TSK_06,_TASKS,US_04,Yukon,truckDelivery,,480,5400,US_04_01_TSK_06_DESC,TASKS\nUS_04_01_TSK_07,US_04_01_TSK_07,_TASKS,US_04,Yukon,truckDelivery,,360,2900,US_04_01_TSK_07_DESC,TASKS\nUS_04_01_TSK_08,US_04_01_TSK_08,_TASKS,US_04,Yukon,truckDelivery,,760,7300,US_04_01_TSK_08_DESC,TASKS\nUS_04_01_TSK_09,US_04_01_TSK_09,_TASKS,US_04,Yukon,truckDelivery,,310,1950,US_04_01_TSK_09_DESC,TASKS\nUS_04_01_TSK_10,US_04_01_TSK_10,_TASKS,US_04,Yukon,truckDelivery,2\u00d7 Wooden Planks,760,8500,US_04_01_TSK_10_DESC,TASKS\nUS_04_01_TSK_11,US_04_01_TSK_11,_TASKS,US_04,Yukon,cargoDelivery,4\u00d7 Barrels Oil,480,2950,US_04_01_TSK_11_DESC,TASKS\nUS_04_01_TSK_12,US_04_01_TSK_12,_TASKS,US_04,Yukon,truckDelivery,,350,3050,US_04_01_TSK_12_DESC,TASKS\nUS_04_01_TSK_13,US_04_01_TSK_13,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Vehicles Spare Parts,330,2400,US_04_01_TSK_13_DESC,TASKS\nUS_04_01_TSK_14,US_04_01_TSK_14,_TASKS,US_04,Yukon,truckDelivery,,420,3600,US_04_01_TSK_14_DESC,TASKS\nUS_04_01_TUBING,US_04_01_TUBING,_CONTRACTS,US_04,Yukon,truckDelivery,,900,10150,US_04_01_TUBING_DESC,CONTRACTS\nUS_04_01_MINE_CONT,US_04_01_MINE_CONT,_CONTRACTS,US_04,Yukon,cargoDelivery,10\u00d7 Metal Roll; 4\u00d7 Concrete Slab; 6\u00d7 Barrels; 10\u00d7 Bricks; 2\u00d7 Forklift Caravan Container 2; 6\u00d7 Metal Planks; 8\u00d7 Wooden Planks; 2\u00d7 Pipes Small; 6\u00d7 Blocks; 2\u00d7 Forklift Caravan Container 2,5630,49000,US_04_01_MINE_CONT_DESC,CONTRACTS\nUS_04_01_SORT_CONT,US_04_01_SORT_CONT,_CONTRACTS,US_04,Yukon,cargoDelivery,4\u00d7 Metal Planks; 6\u00d7 Wooden Planks; 4\u00d7 Blocks; 6\u00d7 Crate Large; 2\u00d7 Forklift Caravan Container 2; 6\u00d7 Metal Roll; 2\u00d7 Concrete Slab; 8\u00d7 Barrels; 8\u00d7 Bricks; 2\u00d7 Forklift Caravan Container 2,4340,36200,US_04_01_SORT_CONT_DESC,CONTRACTS\nUS_04_01_CRAFT_TUTOR,US_04_01_CRAFT_TUTOR,_CONTRACTS,US_04,Yukon,cargoDelivery,1\u00d7 Forklift Caravan Container 2,150,2500,US_04_01_CRAFT_TUTOR_DESC,CONTRACTS\nUS_04_01_TSK_CAT,US_04_01_TSK_CAT,_TASKS,US_04,Yukon,truckDelivery,,680,5800,US_04_01_TSK_CAT_01_DESC,TASKS\nUS_04_01_TSK_FORKLIFT,US_04_01_TSK_FORKLIFT,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Barrels,50,350,US_04_01_TSK_FORKLIFT_DESC,TASKS\nUS_04_01_RACE_L_CNT,US_04_01_RACE_L_CNT,_CONTESTS,US_04,Yukon,exploration,,90,1450,US_04_01_RACE_L_CNT_DESC,CONTESTS\nUS_04_01_RACE_S_CNT,US_04_01_RACE_S_CNT,_CONTESTS,US_04,Yukon,exploration,,60,900,US_04_01_RACE_S_CNT_DESC,CONTESTS\nUS_04_02_FACTORY_BLOCK_01,US_04_02_FACTORY_BLOCK_01,_CONTRACTS,US_04,Yukon,cargoDelivery,6\u00d7 Bricks; 3\u00d7 Concrete Slab; 4\u00d7 Metal Planks; 3\u00d7 Pipes Small; 2\u00d7 Forklift Caravan Container 2; 2\u00d7 Pipe Large,3420,26600,US_04_02_FACTORY_BLOCK_01_DESC,CONTRACTS\nUS_04_02_BLOCKAGE_01_TSK,US_04_02_BLOCKAGE_01_TSK,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Wooden Planks,320,3750,US_04_02_BLOCKAGE_01_TSK_DESC,TASKS\nUS_04_02_FACTORY_BLOCK_02,US_04_02_FACTORY_BLOCK_02,_CONTRACTS,US_04,Yukon,truckDelivery,4\u00d7 Pipes Small; 4\u00d7 Blocks; 2\u00d7 Concrete Slab; 2\u00d7 Metal Planks; 2\u00d7 Forklift Caravan Container 2,2690,21450,US_04_02_FACTORY_BLOCK_03_DESC,CONTRACTS\nUS_04_02_BLOCKAGE_02_TSK,US_04_02_BLOCKAGE_02_TSK,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Wooden Planks,360,4500,US_04_02_BLOCKAGE_02_TSK_DESC,TASKS\nUS_04_02_FACTORY_BLOCK_03,US_04_02_FACTORY_BLOCK_03,_CONTRACTS,US_04,Yukon,truckDelivery,4\u00d7 Metal Planks; 3\u00d7 Pipes Medium; 4\u00d7 Blocks; 4\u00d7 Bricks; 3\u00d7 Forklift Caravan Container 2,2660,20150,US_04_02_FACTORY_BLOCK_02_DESC,CONTRACTS\nUS_04_02_FINAL_DELIVERY,US_04_02_FINAL_DELIVERY,_CONTRACTS,US_04,Yukon,truckDelivery,,1100,13900,US_04_02_FINAL_DELIVERY_DESC,CONTRACTS\nUS_04_02_MED_LOGS,US_04_02_MED_LOGS,_CONTRACTS,US_04,Yukon,cargoDelivery,2\u00d7 Logs Medium; 2\u00d7 Logs Medium; 1\u00d7 Logs Medium; 2\u00d7 Logs Medium,2680,30850,US_04_02_MED_LOGS_DESK,CONTRACTS\nUS_04_02_LOGS_FOR_LUMBERMILL,US_04_02_LOGS_FOR_LUMBERMILL,_CONTRACTS,US_04,Yukon,cargoDelivery,4\u00d7 Logs Medium; 2\u00d7 Logs Long,2480,28950,US_04_02_LOGS_FOR_LUMBERMILL_DESC,CONTRACTS\nUS_04_02_LONG_LOGS,US_04_02_LONG_LOGS,_CONTRACTS,US_04,Yukon,cargoDelivery,2\u00d7 Logs Long; 2\u00d7 Logs Long; 1\u00d7 Logs Long,2500,25100,US_04_02_LONG_LOGS_DESK,CONTRACTS\nUS_04_02_TOOLSDELIVERY_TSK,US_04_02_TOOLSDELIVERY_TSK,_TASKS,US_04,Yukon,truckDelivery,,660,6900,US_04_02_TOOLSDELIVERY_TSK_DESC,TASKS\nUS_04_02_TELEMETRY_TSK,US_04_02_TELEMETRY_TSK,_TASKS,US_04,Yukon,truckDelivery,,800,6100,US_04_02_TELEMETRY_DESC,TASKS\nUS_04_02_DROWNSCOUT_TSK,US_04_02_DROWNSCOUT_TSK,_TASKS,US_04,Yukon,truckDelivery,,590,5550,US_04_02_DROWNSCOUT_TSK_DESC,TASKS\nUS_04_02_BROKEN_TRUCK_TSK,US_04_02_BROKEN_TRUCK_TSK,_TASKS,US_04,Yukon,truckDelivery,,550,5100,US_04_02_BROKEN_TRUCK_TSK_DESC,TASKS\nUS_04_02_FUEL_FOR_HUNTER_TSK,US_04_02_FUEL_FOR_HUNTER_TSK,_TASKS,US_04,Yukon,truckDelivery,,410,4100,US_04_02_FUEL_FOR_HUNTER_TSK_DESC,TASKS\nUS_04_02_DROWNED_TITAN_TSK,US_04_02_DROWNED_TITAN_TSK,_TASKS,US_04,Yukon,truckDelivery,,360,3550,US_04_02_DROWNED_TITAN_TSK_DESC,TASKS\nUS_04_02_OPEN_STORAGE_TSK,US_04_02_OPEN_STORAGE_TSK,_TASKS,US_04,Yukon,truckDelivery,,370,3400,US_04_02_OPEN_STORAGE_TSK_DESC,TASKS\nUS_04_02_HELP_TSK,US_04_02_HELP_TSK,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Crate Large; 1\u00d7 Crate Large; 1\u00d7 Crate Large,850,7700,US_04_02_HELP_TSK_DESC,TASKS\nUS_04_02_STORAGE_TSK,US_04_02_STORAGE_TSK,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Container Small; 1\u00d7 Container Small,820,7550,US_04_02_STORAGE_TSK_DESC,TASKS\nUS_04_02_LOST_CRATES_TSK,US_04_02_LOST_CRATES_TSK,_TASKS,US_04,Yukon,cargoDelivery,5\u00d7 Vehicles Spare Parts,900,7050,US_04_02_LOST_CRATES_TSK_DESC,TASKS\nUS_04_02_TUBES_NEED_NOW_TSK,US_04_02_TUBES_NEED_NOW_TSK,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Pipes Medium,710,5750,US_04_02_TUBES_NEED_NOW_TSK_DESC,TASKS\nUS_04_02_BLOCKED_GATE_TSK,US_04_02_BLOCKED_GATE_TSK,_TASKS,US_04,Yukon,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Metal Planks,590,5700,US_04_02_BLOCKED_GATE_TSK_DESC,TASKS\nUS_04_02_LOST_BIG_CONT_TSK,US_04_02_LOST_BIG_CONT_TSK,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Container Large,650,4600,US_04_02_LOST_BIG_CONT_TSK_DESC,TASKS\nUS_04_02_ELECTROFIX_TSK,US_04_02_ELECTROFIX_TSK,_TASKS,US_04,Yukon,cargoDelivery,1\u00d7 Service Spare Parts,220,1800,US_04_02_ELECTROFIX_TSK_DESC,TASKS\nUS_04_02_STRANGE_SIGNAL_TSK,US_04_02_STRANGE_SIGNAL_TSK,_TASKS,US_04,Yukon,exploration,,200,3400,US_04_02_STRANGE_SIGNAL_TSK_DESC,TASKS\nUS_04_02_DIRT_CNT,US_04_02_DIRT_CNT,_CONTESTS,US_04,Yukon,exploration,,160,2750,US_04_02_DIRT_CNT_DESC,CONTESTS\nUS_04_02_TOTHETOP_CNT,US_04_02_TOTHETOP_CNT,_CONTESTS,US_04,Yukon,exploration,,70,1200,US_04_02_TOTHETOP_CNT_DESC,CONTESTS\nUS_03_01_PAPER_01_CONT,US_03_01_PAPER_01_CONT,_CONTRACTS,US_03,Wisconsin,cargoDelivery,4\u00d7 Metal Planks; 12\u00d7 Metal Roll; 6\u00d7 Barrels; 6\u00d7 Blocks; 2\u00d7 Pipe Large,4170,28450,US_03_01_PAPER_01_CONT_DESC,CONTRACTS\nUS_03_01_LOGS_CONT_01,US_03_01_LOGS_CONT_01,_CONTRACTS,US_03,Wisconsin,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Long; 4\u00d7 Wooden Planks,1070,9350,US_03_01_LOGS_CONT_01_DESC,CONTRACTS\nUS_03_01_BRIDGE_01,US_03_01_BRIDGE_01,_TASKS,US_03,Wisconsin,cargoDelivery,6\u00d7 Metal Planks,1750,13800,US_03_01_BRIDGE_01_DESC,TASKS\nUS_03_01_TSK_01,US_03_01_TSK_01,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Service Spare Parts,420,4700,US_03_01_TSK_01_DESC,TASKS\nUS_03_01_CONT_01,US_03_01_CONT_01,_CONTESTS,US_03,Wisconsin,exploration,,80,1300,US_03_01_CONT_01_DESC,CONTESTS\nUS_03_01_PAPER_02_CONT,US_03_01_PAPER_02_CONT,_CONTRACTS,US_03,Wisconsin,cargoDelivery,12\u00d7 Bricks; 4\u00d7 Bags; 4\u00d7 Concrete Slab; 6\u00d7 Metal Roll,2790,19000,US_03_01_PAPER_02_CONT_DESC,CONTRACTS\nUS_03_01_LOGS_CONT_02,US_03_01_LOGS_CONT_02,_CONTRACTS,US_03,Wisconsin,cargoDelivery,2\u00d7 Logs Long; 6\u00d7 Wooden Planks,1400,14300,US_03_01_LOGS_CONT_02_DESC,CONTRACTS\nUS_03_01_TSK_02,US_03_01_TSK_02,_TASKS,US_03,Wisconsin,truckDelivery,,460,4250,US_03_01_TSK_02_DESC,TASKS\nUS_03_01_BRIDGE_02,US_03_01_BRIDGE_02,_TASKS,US_03,Wisconsin,cargoDelivery,8\u00d7 Wooden Planks,580,6500,US_03_01_BRIDGE_02_DESC,TASKS\nUS_03_01_CONT_02,US_03_01_CONT_02,_CONTESTS,US_03,Wisconsin,exploration,,60,1000,US_03_01_CONT_02_DESC,CONTESTS\nUS_03_01_PAPER_03_CONT,US_03_01_PAPER_03_CONT,_CONTRACTS,US_03,Wisconsin,cargoDelivery,4\u00d7 Barrels; 2\u00d7 Pipes Small; 6\u00d7 Crate Large; 2\u00d7 Container Small; 4\u00d7 Metal Roll; 4\u00d7 Metal Planks; 2\u00d7 Pipe Large; 6\u00d7 Bricks,4810,35500,US_03_01_PAPER_03_CONT_DESC,CONTRACTS\nUS_03_01_LOGS_CONT_03,US_03_01_LOGS_CONT_03,_CONTRACTS,US_03,Wisconsin,cargoDelivery,2\u00d7 Logs Medium; 4\u00d7 Logs Long,1610,12800,US_03_01_LOGS_CONT_03_DESC,CONTRACTS\nUS_03_01_TSK_03,US_03_01_TSK_03,_TASKS,US_03,Wisconsin,truckDelivery,2\u00d7 Vehicles Spare Parts,840,7850,US_03_01_TSK_03_DESC,TASKS\nUS_03_01_BRIDGE_03,US_03_01_BRIDGE_03,_TASKS,US_03,Wisconsin,cargoDelivery,4\u00d7 Metal Planks; 2\u00d7 Concrete Slab,1770,14750,US_03_01_BRIDGE_03_DESC,TASKS\nUS_03_01_LOGS_CONT_04,US_03_01_LOGS_CONT_04,_CONTRACTS,US_03,Wisconsin,cargoDelivery,1\u00d7 Logs Medium; 4\u00d7 Wooden Planks; 2\u00d7 Logs Long,1570,15100,US_03_01_LOGS_CONT_04_DESC,CONTRACTS\nUS_03_01_TSK_04,US_03_01_TSK_04,_TASKS,US_03,Wisconsin,truckDelivery,,380,2900,US_03_01_TSK_04_DESC,TASKS\nUS_03_01_BRIDGE_04,US_03_01_BRIDGE_04,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Metal Planks,700,5000,US_03_01_BRIDGE_04_DESC,TASKS\nUS_03_01_TSK_05,US_03_01_TSK_05,_TASKS,US_03,Wisconsin,cargoDelivery,3\u00d7 Vehicles Spare Parts,570,5800,US_03_01_TSK_05_DESC,TASKS\nUS_03_01_TSK_06,US_03_01_TSK_06,_TASKS,US_03,Wisconsin,truckDelivery,,970,10100,US_03_01_TSK_06_DESC,TASKS\nUS_03_01_TSK_07,US_03_01_TSK_07,_TASKS,US_03,Wisconsin,truckDelivery,,360,2550,US_03_01_TSK_07_DESC,TASKS\nUS_03_01_TSK_08,US_03_01_TSK_08,_TASKS,US_03,Wisconsin,truckDelivery,,630,7200,US_03_01_TSK_08_DESC,TASKS\nUS_03_01_TSK_09,US_03_01_TSK_09,_TASKS,US_03,Wisconsin,truckDelivery,,360,2550,US_03_01_TSK_09_DESC,TASKS\nUS_03_01_TSK_10,US_03_01_TSK_10,_TASKS,US_03,Wisconsin,truckDelivery,1\u00d7 Logs Medium,540,5600,US_03_01_TSK_10_DESC,TASKS\nUS_03_01_TSK_11,US_03_01_TSK_11,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Barrels Oil,560,5950,US_03_01_TSK_11_DESC,TASKS\nUS_03_01_TSK_12,US_03_01_TSK_12,_TASKS,US_03,Wisconsin,cargoDelivery,1\u00d7 Container Large,810,6600,US_03_01_TSK_12_DESC,TASKS\nUS_03_01_CELL_CONT,US_03_01_CELL_CONT,_CONTRACTS,US_03,Wisconsin,cargoDelivery,16\u00d7 Cellulose,2020,17450,US_03_01_CELL_CONT_DESC,CONTRACTS\nUS_03_01_PLANT_CONT,US_03_01_PLANT_CONT,_CONTRACTS,US_03,Wisconsin,cargoDelivery,8\u00d7 Barrels; 2\u00d7 Container Small; 2\u00d7 Metal Planks; 2\u00d7 Crate Large,1960,15850,US_03_01_PLANT_CONT_DESC,CONTRACTS\nUS_03_02_LOST_TRUCK_01_TSK,US_03_02_LOST_TRUCK_01_TSK,_TASKS,US_03,Wisconsin,truckDelivery,,810,6700,US_03_02_LOST_TRUCK_01_TSK_DESC,TASKS\nUS_03_02_BROKEN_BRIDGE_01_TSK,US_03_02_BROKEN_BRIDGE_01_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,1040,8250,US_03_02_BROKEN_BRIDGE_01_DESC,TASKS\nUS_03_02_BROKEN_BRIDGE_02_TSK,US_03_02_BROKEN_BRIDGE_02_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,1000,7550,US_03_02_BROKEN_BRIDGE_02_DESC,TASKS\nUS_03_02_BROKEN_BRIDGE_03_TSK,US_03_02_BROKEN_BRIDGE_03_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Wooden Planks,300,3350,US_03_02_BROKEN_BRIDGE_03_DESC,TASKS\nUS_03_02_TRAIN_OBJ,US_03_02_TRAIN_OBJ,_CONTRACTS,US_03,Wisconsin,truckDelivery,,1480,9350,US_03_02_TRAIN_OBJ_DESC,CONTRACTS\nUS_03_02_BIG_LOGS_OBJ,US_03_02_BIG_LOGS_OBJ,_CONTRACTS,US_03,Wisconsin,cargoDelivery,1\u00d7 Sequoia; 1\u00d7 Sequoia; 1\u00d7 Sequoia,2300,18550,US_03_02_BIG_LOGS_OBJ_DESC,CONTRACTS\nUS_03_02_RAIL_REPAIR_OBJ,US_03_02_RAIL_REPAIR_OBJ,_CONTRACTS,US_03,Wisconsin,cargoDelivery,1\u00d7 Railway; 1\u00d7 Railway; 1\u00d7 Railway; 1\u00d7 Railway; 1\u00d7 Railway; 1\u00d7 Railway,2300,16600,US_03_02_RAIL_REPAIR_OBJ_DESC,CONTRACTS\nUS_03_02_FINAL_COUNTDOWN_OBJ,US_03_02_FINAL_COUNTDOWN_OBJ,_CONTRACTS,US_03,Wisconsin,cargoDelivery,3\u00d7 Metal Planks; 3\u00d7 Service Spare Parts; 3\u00d7 Barrels,1860,14950,US_03_02_FINAL_COUNTDOWN_OBJ_DESC,CONTRACTS\nUS_03_02_CELL_MASHINERY_TSK,US_03_02_CELL_MASHINERY_TSK,_TASKS,US_03,Wisconsin,truckDelivery,1\u00d7 Container Large,1070,8600,US_03_02_CELL_MASHINERY_TSK_DESC,TASKS\nUS_03_02_CARGOINRIVER_TSK,US_03_02_CARGOINRIVER_TSK,_TASKS,US_03,Wisconsin,truckDelivery,2\u00d7 Crate Large,920,7900,US_03_02_CARGOINRIVER_DESC,TASKS\nUS_03_02_TRUCK_IN_RIVER_TSK,US_03_02_TRUCK_IN_RIVER_TSK,_TASKS,US_03,Wisconsin,truckDelivery,,760,7300,US_03_02_TRUCK_IN_RIVER_TSK_DESC,TASKS\nUS_03_02_TOR_TSK,US_03_02_TOR_TSK,_TASKS,US_03,Wisconsin,truckDelivery,,500,5000,US_03_02_TOR_TSK_DESC,TASKS\nUS_03_02_FARMER_LOG_TSK,US_03_02_FARMER_LOG_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Logs Long,1740,17700,US_03_02_FARMER_LOG_TSK_DESC,TASKS\nUS_03_02_DANGER_BARRELS_TSK,US_03_02_DANGER_BARRELS_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,5\u00d7 Barrels Oil,1230,11000,US_03_02_DANGER_BARRELS_TSK_DESC,TASKS\nUS_03_02_TUBE_IN_RIVER_TSK,US_03_02_TUBE_IN_RIVER_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,1\u00d7 Pipe Large,1070,8100,US_03_02_TUBE_IN_RIVER_TSK_DESC,TASKS\nUS_03_02_CARGO_ON_ISLANDS_TSK,US_03_02_CARGO_ON_ISLANDS_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,5\u00d7 Bricks,620,6650,US_03_02_CARGO_ON_ISLANDS_DESC,TASKS\nUS_03_02_INVASION_TSK,US_03_02_INVASION_TSK,_TASKS,US_03,Wisconsin,cargoDelivery,2\u00d7 Vehicles Spare Parts,490,5250,US_03_02_INVASION_DESC,TASKS\nUS_03_02_STRANGER_TSK,US_03_02_STRANGER_TSK,_TASKS,US_03,Wisconsin,exploration,,300,5250,US_03_02_STRANGER_DESC,TASKS\nUS_03_02_YFILES_TSK,US_03_02_YFILES_TSK,_TASKS,US_03,Wisconsin,exploration,,240,4200,US_03_02_YFILES_DESC,TASKS\nUS_03_02_MUDSTER_CNT,US_03_02_MUDSTER_CNT,_CONTESTS,US_03,Wisconsin,exploration,,150,2500,US_03_02_MUDSTER_DESC,CONTESTS\nUS_03_02_SLALOM_CNT,US_03_02_SLALOM_CNT,_CONTESTS,US_03,Wisconsin,exploration,,140,2450,US_03_02_SLALOM_DESC,CONTESTS\nRU_04_01_BRIDGE_01_TSK,RU_04_01_BRIDGE_01_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,950,7900,RU_04_01_BRIDGE_01_TSK_DESC,TASKS\nRU_04_01_ROCKSLIDE_01_TSK,RU_04_01_ROCKSLIDE_01_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Service Spare Parts,420,3550,RU_04_01_ROCKSLIDE_01_DESC,TASKS\nRU_04_01_BRIDGE_02_TSK,RU_04_01_BRIDGE_02_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,890,6850,RU_04_01_BRIDGE_02_TSK_DESC,TASKS\nRU_04_01_ROCKSLIDE_02_TSK,RU_04_01_ROCKSLIDE_02_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Service Spare Parts,540,5700,RU_04_01_ROCKSLIDE_02_DESC,TASKS\nRU_04_01_BRIDGE_03_TSK,RU_04_01_BRIDGE_03_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,950,7950,RU_04_01_BRIDGE_03_TSK_DESC,TASKS\nRU_04_01_ROCKSLIDE_03_TSK,RU_04_01_ROCKSLIDE_03_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Service Spare Parts,570,6200,RU_04_01_ROCKSLIDE_03_DESC,TASKS\nRU_04_01_BRIDGE_04_TSK,RU_04_01_BRIDGE_04_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,970,8300,RU_04_01_BRIDGE_04_TSK_DESC,TASKS\nRU_04_01_TUNNELBLOCK_OBJ,RU_04_01_TUNNELBLOCK_OBJ,_CONTRACTS,RU_04,Amur,truckDelivery,2\u00d7 Metal Planks; 4\u00d7 Wooden Planks; 4\u00d7 Service Spare Parts,1970,17700,RU_04_01_TUNNELBLOCK_DESC,CONTRACTS\nRU_04_01_ROCKET_TRAILER_OBJ,RU_04_01_ROCKET_TRAILER_OBJ,_CONTRACTS,RU_04,Amur,truckDelivery,,1800,15000,RU_04_01_ROCKET_TRAILER_OBJ_DESC,CONTRACTS\nRU_04_01_ROCKET_OBJ,RU_04_01_ROCKET_OBJ,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Rocket Engine; 1\u00d7 Rocket Part 2; 1\u00d7 Rocket Part 1,3090,20450,RU_04_01_ROCKET_OBJ_DESC,CONTRACTS\nRU_04_01_POWERPLANT_OBJ,RU_04_01_POWERPLANT_OBJ,_CONTRACTS,RU_04,Amur,cargoDelivery,3\u00d7 Metal Planks; 1\u00d7 Container Large,1630,12250,RU_04_01_POWERPLANT_OBJ_DESC,CONTRACTS\nRU_04_01_HELP_TSK,RU_04_01_HELP_TSK,_TASKS,RU_04,Amur,truckDelivery,1\u00d7 Blocks; 1\u00d7 Concrete Slab; 1\u00d7 Bricks,1250,12750,RU_04_01_HELP_DESC,TASKS\nRU_04_01_EXPEDITION_TSK,RU_04_01_EXPEDITION_TSK,_TASKS,RU_04,Amur,truckDelivery,1\u00d7 Forklift Caravan Container 2,1250,10300,RU_04_01_EXPEDITION_TSK_DESC,TASKS\nRU_04_01_FACTORY_TSK,RU_04_01_FACTORY_TSK,_TASKS,RU_04,Amur,truckDelivery,5\u00d7 Vehicles Spare Parts,950,8700,RU_04_01_FACTORY_TSK_DESC,TASKS\nRU_04_01_LOST_TRAILER_TSK,RU_04_01_LOST_TRAILER_TSK,_TASKS,RU_04,Amur,truckDelivery,,380,3600,RU_04_01_LOST_TRAILER_TSK_DESC,TASKS\nRU_04_01_TRUCK_ON_HILL_TSK,RU_04_01_TRUCK_ON_HILL_TSK,_TASKS,RU_04,Amur,truckDelivery,,320,2500,RU_04_01_TRUCK_ON_HILL_TSK_DESC,TASKS\nRU_04_01_SCOUT_IN_TROUBLE_TSK,RU_04_01_SCOUT_IN_TROUBLE_TSK,_TASKS,RU_04,Amur,truckDelivery,,350,2350,RU_04_01_SCOUT_IN_TROUBLE_DESC,TASKS\nRU_04_01_FUELTASK_TSK,RU_04_01_FUELTASK_TSK,_TASKS,RU_04,Amur,truckDelivery,,290,2050,RU_04_01_FUELTASK_TSK_DESC,TASKS\nRU_04_01_CHURCH_TSK,RU_04_01_CHURCH_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Logs Medium; 1\u00d7 Logs Long; 1\u00d7 Metal Planks,1050,7350,RU_04_01_CHURCH_DESC,TASKS\nRU_04_01_SUPPLIES_TSK,RU_04_01_SUPPLIES_TSK,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 1\u00d7 Container Small; 1\u00d7 Crate Large,1040,6900,RU_04_01_SUPPLIES_TSK_DESC,TASKS\nRU_04_01_CARGOCULT_TSK,RU_04_01_CARGOCULT_TSK,_TASKS,RU_04,Amur,cargoDelivery,3\u00d7 Crate Large,560,4000,RU_04_01_CARGOCULT_TSK_DESC,TASKS\nRU_04_01_SAWMILL_TSK,RU_04_01_SAWMILL_TSK,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Container Small,400,3900,RU_04_01_SAWMILL_TSK_DESC,TASKS\nRU_04_01_ROCK_RACE_CNT,RU_04_01_ROCK_RACE_CNT,_CONTESTS,RU_04,Amur,exploration,,100,1750,RU_04_01_ROCK_RACE_CNT_DESC,CONTESTS\nRU_04_01_ICE_RACE_CNT,RU_04_01_ICE_RACE_CNT,_CONTESTS,RU_04,Amur,exploration,,80,1250,RU_04_01_ICE_RACE_CNT_DESC,CONTESTS\nRU_04_02_ROCK_CLEANUP_1_TSK,RU_04_02_ROCK_CLEANUP_1_TSK,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Metal Planks,470,4500,RU_04_02_ROCK_CLEANUP_1_TSK_DESC,TASKS\nRU_04_02_TREE_CLEANUP_1_TSK,RU_04_02_TREE_CLEANUP_1_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Service Spare Parts,400,2800,RU_04_02_TREE_CLEANUP_1_TSK_DESC,TASKS\nRU_04_02_ROCK_CLEANUP_2_TSK,RU_04_02_ROCK_CLEANUP_2_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,720,5400,RU_04_02_ROCK_CLEANUP_2_TSK_DESC,TASKS\nRU_04_02_TREE_CLEANUP_2_TSK,RU_04_02_TREE_CLEANUP_2_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Service Spare Parts,420,3150,RU_04_02_TREE_CLEANUP_2_TSK_DESC,TASKS\nRU_04_02_TREE_CLEANUP_3_TSK,RU_04_02_TREE_CLEANUP_3_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Service Spare Parts,460,3700,RU_04_02_TREE_CLEANUP_3_TSK_DESC,TASKS\nRU_04_02_ROCKET_TRAIN_DELIVERY_OBJ,RU_04_02_ROCKET_TRAIN_DELIVERY_OBJ,_CONTRACTS,RU_04,Amur,truckDelivery,,1070,29000,RU_04_02_ROCKET_TRAIN_DELIVERY_OBJ_DESC,CONTRACTS\nRU_04_02_MISSION_CONTROL_OBJ,RU_04_02_MISSION_CONTROL_OBJ,_CONTRACTS,RU_04,Amur,truckDelivery,2\u00d7 Bricks; 1\u00d7 Concrete Slab,1290,12250,RU_04_02_MISSION_CONTROL_OBJ_DESC,CONTRACTS\nRU_04_02_ROCKET_DELIVERY_OBJ,RU_04_02_ROCKET_DELIVERY_OBJ,_CONTRACTS,RU_04,Amur,truckDelivery,,720,9600,RU_04_02_ROCKET_DELIVERY_OBJ_DESC,CONTRACTS\nRU_04_02_FUEL_LINE_REPAIR_OBJ,RU_04_02_FUEL_LINE_REPAIR_OBJ,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Pipe Large,1610,12500,RU_04_02_FUEL_LINE_REPAIR_OBJ_DESC,CONTRACTS\nRU_04_02_TOWN_RESUPPLY_TSK,RU_04_02_TOWN_RESUPPLY_TSK,_TASKS,RU_04,Amur,truckDelivery,,1090,10100,RU_04_02_TOWN_RESUPPLY_TSK_DESC,TASKS\nRU_04_02_DAN_RESCUE_TSK,RU_04_02_DAN_RESCUE_TSK,_TASKS,RU_04,Amur,truckDelivery,,760,7200,RU_04_02_DAN_RESCUE_TSK_DESC,TASKS\nRU_04_02_FISHERMAN_SCOUT_RESCUE_TSK,RU_04_02_FISHERMAN_SCOUT_RESCUE_TSK,_TASKS,RU_04,Amur,truckDelivery,,450,4500,RU_04_02_FISHERMAN_SCOUT_RESCUE_TSK_DESC,TASKS\nRU_04_02_KHAN_RECOVER_TSK,RU_04_02_KHAN_RECOVER_TSK,_TASKS,RU_04,Amur,truckDelivery,,410,3750,RU_04_02_KHAN_RECOVER_TSK_DESC,TASKS\nRU_04_02_DELIVERY_TRUCK_RECOVER_TSK,RU_04_02_DELIVERY_TRUCK_RECOVER_TSK,_TASKS,RU_04,Amur,truckDelivery,,370,3400,RU_04_02_DELIVERY_TRUCK_RECOVER_TSK_DESC,TASKS\nRU_04_02_ABANDONED_TRUCK_RECOVERY_TSK,RU_04_02_ABANDONED_TRUCK_RECOVERY_TSK,_TASKS,RU_04,Amur,truckDelivery,,320,2550,RU_04_02_ABANDONED_TRUCK_RECOVERY_TSK_DESC,TASKS\nRU_04_02_FARM_RESTOCK_BRICKS_TSK,RU_04_02_FARM_RESTOCK_BRICKS_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Bricks; 2\u00d7 Metal Planks; 3\u00d7 Logs Short,1400,13050,RU_04_02_FARM_RESTOCK_BRICKS_TSK_DESC,TASKS\nRU_04_02_LOG_BRIDGE_CONSTRUCTION_TSK,RU_04_02_LOG_BRIDGE_CONSTRUCTION_TSK,_TASKS,RU_04,Amur,cargoDelivery,3\u00d7 Logs Medium,560,5550,RU_04_02_LOG_BRIDGE_CONSTRUCTION_TSK_DESC,TASKS\nRU_04_02_METEO_RESTOCK_TSK,RU_04_02_METEO_RESTOCK_TSK,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Barrels,390,4750,RU_04_02_METEO_RESTOCK_TSK_DESC,TASKS\nRU_04_02_CHURCH_REPAIR_TSK,RU_04_02_CHURCH_REPAIR_TSK,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Wooden Planks,330,4000,RU_04_02_CHURCH_REPAIR_TSK_DESC,TASKS\nRU_04_02_BRIDGE_CONSTRUCTION_TSK,RU_04_02_BRIDGE_CONSTRUCTION_TSK,_TASKS,RU_04,Amur,cargoDelivery,3\u00d7 Wooden Planks,350,3950,RU_04_02_BRIDGE_CONSTRUCTION_TSK_DESC,TASKS\nRU_04_02_FARM_FUEL_RESTOCK_TSK,RU_04_02_FARM_FUEL_RESTOCK_TSK,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Barrels,380,3650,RU_04_02_FARM_FUEL_RESTOCK_TSK_DESC,TASKS\nRU_04_02_PICTURESCUE_TSK,RU_04_02_PICTURESCUE_TSK,_TASKS,RU_04,Amur,exploration,,450,7750,RU_04_02_PICTURESCUE_TSK_DESC,TASKS\nRU_04_02_ABANDON_CHECKUP_TSK,RU_04_02_ABANDON_CHECKUP_TSK,_TASKS,RU_04,Amur,exploration,,360,6250,RU_04_02_ABANDON_CHECKUP_TSK_DESC,TASKS\nRU_04_02_ZIKZ_RESCUE_TSK,RU_04_02_ZIKZ_RESCUE_TSK,_TASKS,RU_04,Amur,exploration,,330,2750,RU_04_02_ZIKZ_RESCUE_TSK_DESC,TASKS\nRU_04_02_FUEL_RESTOCK_TARGET_CNT,RU_04_02_FUEL_RESTOCK_TARGET_CNT,_CONTESTS,RU_04,Amur,truckDelivery,,260,2150,RU_04_02_FUEL_RESTOCK_TARGET_CNT_DESC,CONTESTS\nRU_04_02_FUEL_RUN_CNT,RU_04_02_FUEL_RUN_CNT,_CONTESTS,RU_04,Amur,exploration,,150,2600,RU_04_02_FUEL_RUN_CNT_DESC,CONTESTS\nRU_04_03_SUBSTATION_1,RU_04_03_SUBSTATION_1,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,930,8950,RU_04_03_SUBSTATION_DESC,CONTRACTS\nRU_04_03_BRIDGE_BUILDING_01,RU_04_03_BRIDGE_BUILDING_01,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Wooden Planks,930,10950,RU_04_03_BRIDGE_BUILDING_01_DESC,TASKS\nRU_04_03_SCOUT_01,RU_04_03_SCOUT_01,_TASKS,RU_04,Amur,exploration,,440,7650,RU_04_03_SCOUT_1_DESC,TASKS\nRU_04_03_BRIDGE_BUILDING_02,RU_04_03_BRIDGE_BUILDING_02,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Wooden Planks,1020,11750,RU_04_03_BRIDGE_BUILDING_02_DESC,TASKS\nRU_04_03_OBSTACLE_02,RU_04_03_OBSTACLE_02,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Metal Planks,800,8650,RU_04_03_OBSTACLE_02_DESC,TASKS\nRU_04_03_SCOUT_02,RU_04_03_SCOUT_02,_TASKS,RU_04,Amur,exploration,,480,8250,RU_04_03_SCOUT_2_DESC,TASKS\nRU_04_03_BRIDGE_BUILDING_03,RU_04_03_BRIDGE_BUILDING_03,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks,480,6600,RU_04_03_BRIDGE_BUILDING_03_DESC,TASKS\nRU_04_03_OBSTACLE_03,RU_04_03_OBSTACLE_03,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,730,6450,RU_04_03_OBSTACLE_03_METAL_DESC,TASKS\nRU_04_03_OBSTACLE_04,RU_04_03_OBSTACLE_04,_TASKS,RU_04,Amur,truckDelivery,,420,3600,RU_04_03_OBSTACLE_04_DESC,TASKS\nRU_04_03_OBSTACLE_05,RU_04_03_OBSTACLE_05,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Wooden Planks,330,2300,RU_04_03_OBSTACLE_05_DESC,TASKS\nRU_04_03_OBSTACLE_06,RU_04_03_OBSTACLE_06,_TASKS,RU_04,Amur,cargoDelivery,1\u00d7 Metal Planks,360,2900,RU_04_03_OBSTACLE_06_DESC,TASKS\nRU_04_03_GIANT,RU_04_03_GIANT,_CONTRACTS,RU_04,Amur,truckDelivery,,420,4250,RU_04_03_GIANT_DESC,CONTRACTS\nRU_04_03_FUEL,RU_04_03_FUEL,_CONTRACTS,RU_04,Amur,truckDelivery,,420,3950,RU_04_03_FUEL_DESC,CONTRACTS\nRU_04_03_LOST_C,RU_04_03_LOST_C,_CONTRACTS,RU_04,Amur,truckDelivery,,380,2800,RU_04_03_LOST_C_DESC,CONTRACTS\nRU_04_03_SUPPLIES,RU_04_03_SUPPLIES,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Container Small; 2\u00d7 Wooden Planks,1020,11250,RU_04_03_SUPPLIES_DESC,CONTRACTS\nRU_04_03_FALLEN_POWER_LINES,RU_04_03_FALLEN_POWER_LINES,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Blocks; 1\u00d7 Metal Planks,830,9650,RU_04_03_FALLEN_POWER_LINES_TSK_DESC,CONTRACTS\nRU_04_03_SEISMIC,RU_04_03_SEISMIC,_CONTRACTS,RU_04,Amur,exploration,,380,6650,RU_04_03_SEISMIC_DESC,CONTRACTS\nRU_04_03_LOST_B,RU_04_03_LOST_B,_TASKS,RU_04,Amur,truckDelivery,,470,5100,RU_04_03_LOST_B_DESC,TASKS\nRU_04_03_RUN,RU_04_03_RUN,_CONTESTS,RU_04,Amur,exploration,,100,1600,RU_04_03_RUN_DESC,CONTESTS\nRU_04_03_OFF_ROAD,RU_04_03_OFF_ROAD,_CONTESTS,RU_04,Amur,exploration,,80,1350,RU_04_03_OFF_ROAD_DESC,CONTESTS\nRU_04_04_NPZ_RESTORATION_C,RU_04_04_NPZ_RESTORATION_C,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Pipes Small; 2\u00d7 Concrete Slab; 2\u00d7 Metal Planks; 6\u00d7 Bricks; 1\u00d7 Pipe Large; 1\u00d7 Pipes Medium; 2\u00d7 Barrels Oil; 2\u00d7 Pipes Small,3790,27200,RU_04_04_NPZ_RESTORATION_C_DESC,CONTRACTS\nRU_04_04_NPZ_RESTORATION_A,RU_04_04_NPZ_RESTORATION_A,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Blocks; 4\u00d7 Bags; 2\u00d7 Barrels Oil; 2\u00d7 Bags 2; 2\u00d7 Metal Planks; 2\u00d7 Pipes Medium; 2\u00d7 Wooden Planks; 2\u00d7 Blocks,3610,25900,RU_04_04_NPZ_RESTORATION_A_DESC,CONTRACTS\nRU_04_04_NPZ_RESTORATION_B,RU_04_04_NPZ_RESTORATION_B,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Pipe Large; 1\u00d7 Pipes Medium; 2\u00d7 Pipes Small; 1\u00d7 Concrete Slab; 2\u00d7 Wooden Planks; 2\u00d7 Metal Planks; 4\u00d7 Bags,3360,24150,RU_04_04_NPZ_RESTORATION_B_DESC,CONTRACTS\nRU_04_04_GARAGE_REPAIR,RU_04_04_GARAGE_REPAIR,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Roll; 4\u00d7 Pipes Small; 3\u00d7 Bricks; 2\u00d7 Blocks,2210,18800,RU_04_04_GARAGE_REPAIR_DESK,CONTRACTS\nRU_04_04_WINTER_SUPPLIES,RU_04_04_WINTER_SUPPLIES,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Logs Medium; 4\u00d7 Logs Long; 2\u00d7 Logs Short,2080,17850,RU_04_04_WINTER_SUPPLIES_DESK,CONTRACTS\nRU_04_04_FACTORY_RESTORATION,RU_04_04_FACTORY_RESTORATION,_CONTRACTS,RU_04,Amur,cargoDelivery,4\u00d7 Bricks; 2\u00d7 Blocks; 2\u00d7 Metal Roll; 4\u00d7 Bags; 2\u00d7 Metal Planks,2250,17300,RU_04_04_FACTORY_RESTORATION_DESC,CONTRACTS\nRU_04_04_OLD_TRAIN,RU_04_04_OLD_TRAIN,_CONTRACTS,RU_04,Amur,cargoDelivery,6\u00d7 Logs Medium,1500,15950,RU_04_04_OLD_TRAIN_DESC,CONTRACTS\nRU_04_04_HEATING_SEASON,RU_04_04_HEATING_SEASON,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Logs Long; 2\u00d7 Logs Short,1500,15300,RU_04_04_HEATING_SEASON_DESK,CONTRACTS\nRU_04_04_WAREHOUSE_RESTORATION,RU_04_04_WAREHOUSE_RESTORATION,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Bricks; 1\u00d7 Blocks; 2\u00d7 Metal Planks,1850,13100,RU_04_04_WAREHOUSE_RESTORATION_DESC,CONTRACTS\nRU_04_04_LUMBER_MILL_RESTORATION,RU_04_04_LUMBER_MILL_RESTORATION,_CONTRACTS,RU_04,Amur,cargoDelivery,4\u00d7 Bricks; 2\u00d7 Blocks; 2\u00d7 Metal Roll; 2\u00d7 Bags 2,1470,10650,RU_04_04_LUMBER_MILL_RESTORATION_DESK,CONTRACTS\nRU_04_04_FISHER,RU_04_04_FISHER,_CONTRACTS,RU_04,Amur,cargoDelivery,2\u00d7 Logs Short; 1\u00d7 Logs Medium,690,6000,RU_04_04_FISHER_DESC,CONTRACTS\nRU_04_04_LOCAL_RESIDENTS,RU_04_04_LOCAL_RESIDENTS,_CONTRACTS,RU_04,Amur,cargoDelivery,1\u00d7 Container Large,730,5950,RU_04_04_LOCAL_RESIDENTS_DESK,CONTRACTS\nRU_04_04_WOOD_CARVING,RU_04_04_WOOD_CARVING,_CONTRACTS,RU_04,Amur,cargoDelivery,4\u00d7 Logs Medium,570,5850,RU_04_04_WOOD_CARVING_DESC,CONTRACTS\nRU_04_04_EXPLORATIONS_WORKS,RU_04_04_EXPLORATIONS_WORKS,_CONTRACTS,RU_04,Amur,exploration,,210,3600,RU_04_04_EXPLORATIONS_WORKS_DESK,CONTRACTS\nRU_04_04_TSK_REALLY_BIG_ONE,RU_04_04_TSK_REALLY_BIG_ONE,_TASKS,RU_04,Amur,truckDelivery,5\u00d7 Vehicles Spare Parts,940,8650,RU_04_04_TSK_REALLY_BIG_ONE_01_DESC,TASKS\nRU_04_04_TSK_DREAM_CAR,RU_04_04_TSK_DREAM_CAR,_TASKS,RU_04,Amur,truckDelivery,,530,5800,RU_04_04_TSK_DREAM_CAR_DESC,TASKS\nRU_04_04_TSK_DANGEROUS_ROAD,RU_04_04_TSK_DANGEROUS_ROAD,_TASKS,RU_04,Amur,truckDelivery,1\u00d7 Logs Medium,690,5600,RU_04_04_TSK_DANGEROUS_ROAD_01_DESC,TASKS\nRU_04_04_TSK_BIG_AND_ABANDONED,RU_04_04_TSK_BIG_AND_ABANDONED,_TASKS,RU_04,Amur,truckDelivery,,530,5400,RU_04_04_TSK_BIG_AND_ABANDONED_01_DESC,TASKS\nRU_04_04_TSK_OLD_BUT_GOLD,RU_04_04_TSK_OLD_BUT_GOLD,_TASKS,RU_04,Amur,truckDelivery,,460,4600,RU_04_04_TSK_OLD_BUT_GOLD_01_DESC,TASKS\nRU_04_04_TSK_FAMILY_BUSINESS,RU_04_04_TSK_FAMILY_BUSINESS,_TASKS,RU_04,Amur,truckDelivery,,370,3000,RU_04_04_TSK_FAMILY_BUSINESS_01_DESC,TASKS\nRU_04_04_TSK_UNLUCKY_BOY,RU_04_04_TSK_UNLUCKY_BOY,_TASKS,RU_04,Amur,truckDelivery,,370,3000,RU_04_04_TSK_UNLUCKY_BOY_01_DESC,TASKS\nRU_04_04_TSK_NEIGHBOR_HELP,RU_04_04_TSK_NEIGHBOR_HELP,_TASKS,RU_04,Amur,truckDelivery,,320,2250,RU_04_04_TSK_NEIGHBOR_HELP_01_DESC,TASKS\nRU_04_04_TSK_BRIDGE_A,RU_04_04_TSK_BRIDGE_A,_TASKS,RU_04,Amur,cargoDelivery,3\u00d7 Metal Roll; 2\u00d7 Metal Planks,1150,8000,RU_04_04_TSK_BRIDGE_A_01_DESK,TASKS\nRU_04_04_TSK_ROCKS_C,RU_04_04_TSK_ROCKS_C,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,920,7900,RU_04_04_TSK_ROCKS_C_01_DESC,TASKS\nRU_04_04_TSK_ROCKS_A,RU_04_04_TSK_ROCKS_A,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,890,7350,RU_04_04_TSK_ROCKS_A_01_DESC,TASKS\nRU_04_04_TSK_BRIDGE_B,RU_04_04_TSK_BRIDGE_B,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Logs Medium; 4\u00d7 Wooden Planks,740,6350,RU_04_04_TSK_BRIDGE_B_01_DESK,TASKS\nRU_04_04_TSK_ROCKS_B,RU_04_04_TSK_ROCKS_B,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,810,6000,RU_04_04_TSK_ROCKS_B_01_DESC,TASKS\nRU_04_04_TSK_ROCKS_D,RU_04_04_TSK_ROCKS_D,_TASKS,RU_04,Amur,cargoDelivery,2\u00d7 Metal Planks,710,4200,RU_04_04_TSK_ROCKS_D_01_DESC,TASKS\nRU_04_04_TSK_STEEL_MACHINE,RU_04_04_TSK_STEEL_MACHINE,_TASKS,RU_04,Amur,exploration,,490,5150,RU_04_04_TSK_STEEL_MACHINE_DESC,TASKS\nRU_04_04__RACE_C_CNT,RU_04_04__RACE_C_CNT,_CONTESTS,RU_04,Amur,exploration,,100,1750,RU_04_04_RACE_C_CNT_DESC,CONTESTS\nRU_04_04__RACE_B_CNT,RU_04_04__RACE_B_CNT,_CONTESTS,RU_04,Amur,exploration,,90,1500,RU_04_04_RACE_B_CNT_DESC,CONTESTS\nRU_04_04_RACE_A_CNT,RU_04_04_RACE_A_CNT,_CONTESTS,RU_04,Amur,exploration,,80,1350,RU_04_04_RACE_A_CNT_DESC,CONTESTS\nRU_04_04__RACE_D_CNT,RU_04_04__RACE_D_CNT,_CONTESTS,RU_04,Amur,exploration,,60,1000,RU_04_04_RACE_D_CNT_DESC,CONTESTS\nRU_05_01_CAR_FACTORY_01,RU_05_01_CAR_FACTORY_01,_CONTRACTS,RU_05,Don,truckDelivery,4\u00d7 Bricks,1000,10100,RU_05_01_CAR_FACTORY_01_DESC,CONTRACTS\nRU_05_01_DELIVERY_01,RU_05_01_DELIVERY_01,_TASKS,RU_05,Don,cargoDelivery,2\u00d7 Wooden Planks,280,3300,RU_05_01_DELIVERY_01_DESC,TASKS\nRU_05_01_TOWER_01,RU_05_01_TOWER_01,_TASKS,RU_05,Don,cargoDelivery,1\u00d7 Container Small,550,3250,RU_05_01_TOWER_01_DESC,TASKS\nRU_05_01_BOILER_ROOM_02,RU_05_01_BOILER_ROOM_02,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Metal Roll; 1\u00d7 Boiler,1900,15900,RU_05_01_BOILER_ROOM_02_DESC,CONTRACTS\nRU_05_01_WAREHOUSE_03,RU_05_01_WAREHOUSE_03,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Container Large; 5\u00d7 Vehicles Spare Parts,960,7950,RU_05_01_WAREHOUSE_03_DESC,CONTRACTS\nRU_05_01_RECOVERY_PIPES,RU_05_01_RECOVERY_PIPES,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Pipe Large; 1\u00d7 Pipe Large,2280,19900,RU_05_01_RECOVERY_PIPES_DESC,CONTRACTS\nRU_05_01_RESTORATION_VILLAGE,RU_05_01_RESTORATION_VILLAGE,_CONTRACTS,RU_05,Don,cargoDelivery,1\u00d7 Pipes Small; 1\u00d7 Pipes Small; 1\u00d7 Pipes Small,870,8600,RU_05_01_RESTORATION_VILLAGE_DESC,CONTRACTS\nRU_05_01_PORT_RECOVERY,RU_05_01_PORT_RECOVERY,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Blocks; 3\u00d7 Bags,960,8550,RU_05_01_PORT_RECOVERY_DELIVERY_DESC,CONTRACTS\nRU_05_01_PORT_RECOVERY_ENERGY,RU_05_01_PORT_RECOVERY_ENERGY,_CONTRACTS,RU_05,Don,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Metal Planks,630,4700,RU_05_01_PORT_RECOVERY_ENERGY_DESC,CONTRACTS\nRU_05_01_TRACK_DELIVERY,RU_05_01_TRACK_DELIVERY,_TASKS,RU_05,Don,truckDelivery,,280,2550,RU_05_01_TRACK_DELIVERY_DESC,TASKS\nRU_05_01_BRIDGE,RU_05_01_BRIDGE,_TASKS,RU_05,Don,truckDelivery,,290,2450,RU_05_01_BRIDGE_DESC,TASKS\nRU_05_01_WOODEN_BRIDGE,RU_05_01_WOODEN_BRIDGE,_TASKS,RU_05,Don,cargoDelivery,2\u00d7 Wooden Planks,640,7250,RU_05_01_WOODEN_BRIDGE_DESC,TASKS\nRU_05_01_BIG_BRIDGE,RU_05_01_BIG_BRIDGE,_TASKS,RU_05,Don,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Bags,750,7000,RU_05_01_BIG_BRIDGE_DESC,TASKS\nRU_05_01_FUEL,RU_05_01_FUEL,_TASKS,RU_05,Don,cargoDelivery,3\u00d7 Barrels; 2\u00d7 Service Spare Parts,690,5000,RU_05_01_FUEL_DESC,TASKS\nRU_05_01_TRACK_TATRA,RU_05_01_TRACK_TATRA,_TASKS,RU_05,Don,cargoDelivery,1\u00d7 Metal Planks,640,4900,RU_05_01_TRACK_TATRA_DESC,TASKS\nRU_05_01_STONES,RU_05_01_STONES,_TASKS,RU_05,Don,cargoDelivery,2\u00d7 Metal Planks,340,2200,RU_05_01_STONES_DESC,TASKS\nRU_05_01_SWAMP_RACE,RU_05_01_SWAMP_RACE,_CONTESTS,RU_05,Don,exploration,,50,600,RU_05_01_SWAMP_RACE_DESC,CONTESTS\nRU_05_01_FACTORY_RACE,RU_05_01_FACTORY_RACE,_CONTESTS,RU_05,Don,exploration,,100,400,RU_05_01_FACTORY_RACE_DESC,CONTESTS\nRU_05_02_BIG_BRIDGE_01,RU_05_02_BIG_BRIDGE_01,_CONTRACTS,RU_05,Don,cargoDelivery,4\u00d7 Bags,610,4300,RU_05_02_BIG_BRIDGE_01_DESC,CONTRACTS\nRU_05_02_ROCK_01,RU_05_02_ROCK_01,_TASKS,RU_05,Don,truckDelivery,,280,2250,RU_05_02_ROCK_01_DESC,TASKS\nRU_05_02_BRIDGE_01,RU_05_02_BRIDGE_01,_TASKS,RU_05,Don,cargoDelivery,4\u00d7 Wooden Planks; 2\u00d7 Logs Long,990,8900,RU_05_02_BRIDGE_01_DESC,TASKS\nRU_05_02_DELIVERY_01,RU_05_02_DELIVERY_01,_TASKS,RU_05,Don,cargoDelivery,1\u00d7 Radioctive,210,2500,RU_05_02_DELIVERY_01_DESC,TASKS\nRU_05_02_SCOUT_01,RU_05_02_SCOUT_01,_TASKS,RU_05,Don,exploration,,180,1800,RU_05_02_SCOUT_01_DESC,TASKS\nRU_05_02_BIG_BRIDGE_02,RU_05_02_BIG_BRIDGE_02,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Concrete Slab; 2\u00d7 Iron Road,3400,28300,RU_05_02_BIG_BRIDGE_02_DESC,CONTRACTS\nRU_05_02_BRIDGE_02,RU_05_02_BRIDGE_02,_TASKS,RU_05,Don,cargoDelivery,3\u00d7 Logs Medium,710,2100,RU_05_02_BRIDGE_02_DESC,TASKS\nRU_05_02_DELIVERY_02,RU_05_02_DELIVERY_02,_TASKS,RU_05,Don,exploration,,60,1050,RU_05_02_DELIVERY_02_DESC,TASKS\nRU_05_02_RESTORATION_VILLAGE_WORKSHOP,RU_05_02_RESTORATION_VILLAGE_WORKSHOP,_CONTRACTS,RU_05,Don,truckDelivery,,400,4300,RU_05_02_RESTORATION_VILLAGE_WORKSHOP_DESC,CONTRACTS\nRU_05_02_RESTORATION_VILLAGE_REPAIR,RU_05_02_RESTORATION_VILLAGE_REPAIR,_CONTRACTS,RU_05,Don,truckDelivery,,280,2250,RU_05_02_RESTORATION_VILLAGE_REPAIR_DESC,CONTRACTS\nRU_05_02_RAIL_REPAIR,RU_05_02_RAIL_REPAIR,_CONTRACTS,RU_05,Don,cargoDelivery,1\u00d7 Railway; 1\u00d7 Railway; 1\u00d7 Railway,1810,20200,RU_05_02_RAIL_REPAIR_DESC,CONTRACTS\nRU_05_02_RESTORATION_VILLAGE_PUMPTOWER,RU_05_02_RESTORATION_VILLAGE_PUMPTOWER,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Bags; 4\u00d7 Metal Roll,740,6500,RU_05_02_RESTORATION_VILLAGE_PUMPTOWER_DESC,CONTRACTS\nRU_05_02_RESTORATION_VILLAGE_SUPPLIES,RU_05_02_RESTORATION_VILLAGE_SUPPLIES,_CONTRACTS,RU_05,Don,cargoDelivery,2\u00d7 Container Small,570,5650,RU_05_02_RESTORATION_VILLAGE_SUPPLIES_DESC,CONTRACTS\nRU_05_02_MAIL,RU_05_02_MAIL,_CONTRACTS,RU_05,Don,exploration,,230,4000,RU_05_02_MAIL_DESC,CONTRACTS\nRU_05_02_LAKE_RACE,RU_05_02_LAKE_RACE,_CONTESTS,RU_05,Don,exploration,,200,800,RU_05_02_LAKE_RACE_DESC,CONTESTS\nRU_05_02_HONEY,RU_05_02_HONEY,_CONTESTS,RU_05,Don,exploration,,200,500,RU_05_02_HONEY_DESC,CONTESTS\nUS_06_01_LOGGING_01,US_06_01_LOGGING_01,_CONTRACTS,US_06,Maine,cargoDelivery,1\u00d7 Wooden Planks Medium; 2\u00d7 Logs Short; 2\u00d7 Wooden Planks Medium; 2\u00d7 Logs Short; 1\u00d7 Logs Medium; 1\u00d7 Logs Long; 1\u00d7 Wooden Planks Long,2820,27150,US_06_01_LOGGING_01_DESC,CONTRACTS\nUS_06_01_GARAGE_STAGE_01_SLOTS,US_06_01_GARAGE_STAGE_01_SLOTS,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Blocks; 1\u00d7 Container Large,1540,13400,US_06_01_GARAGE_STAGE_01_SLOTS_DESC,CONTRACTS\nUS_06_01_GARAGE_STAGE_01_REPAIR,US_06_01_GARAGE_STAGE_01_REPAIR,_CONTRACTS,US_06,Maine,cargoDelivery,2\u00d7 Vehicles Spare Parts; 4\u00d7 Barrels; 4\u00d7 Bricks,1390,13050,US_06_01_GARAGE_STAGE_01_REPAIR_DESC,CONTRACTS\nUS_06_01_TSK_01,US_06_01_TSK_01,_TASKS,US_06,Maine,cargoDelivery,3\u00d7 Barrels Oil,720,8200,US_06_01_TSK_01_DESC,TASKS\nUS_06_01_ROCKS_01,US_06_01_ROCKS_01,_TASKS,US_06,Maine,cargoDelivery,4\u00d7 Wooden Planks,410,4700,US_06_01_ROCKS_01_DESC,TASKS\nUS_06_01_CON_01,US_06_01_CON_01,_CONTESTS,US_06,Maine,exploration,,110,1900,US_06_01_CON_01_DESC,CONTESTS\nUS_06_01_MILL_02_STUFF,US_06_01_MILL_02_STUFF,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Metal Planks; 4\u00d7 Bags; 2\u00d7 Concrete Slab; 4\u00d7 Forklift Caravan Container 2,3990,37200,US_06_01_MILL_02_STUFF_DESC,CONTRACTS\nUS_06_01_LOGGING_02,US_06_01_LOGGING_02,_CONTRACTS,US_06,Maine,cargoDelivery,1\u00d7 Wooden Planks Long; 2\u00d7 Logs Medium; 2\u00d7 Wooden Planks; 2\u00d7 Logs Short; 1\u00d7 Wooden Planks Long; 2\u00d7 Logs Medium,2170,21100,US_06_01_LOGGING_02_DESC,CONTRACTS\nUS_06_01_GARAGE_STAGE_02_TRUCKSTORE,US_06_01_GARAGE_STAGE_02_TRUCKSTORE,_CONTRACTS,US_06,Maine,cargoDelivery,2\u00d7 Container Small; 2\u00d7 Concrete Slab,1660,14100,US_06_01_GARAGE_STAGE_02_TRUCKSTORE_DESC,CONTRACTS\nUS_06_01_GARAGE_CONT_02_TUNING,US_06_01_GARAGE_CONT_02_TUNING,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Metal Roll; 2\u00d7 Metal Planks,1220,9650,US_06_01_GARAGE_CONT_02_TUNING_DESC,CONTRACTS\nUS_06_01_TSK_02,US_06_01_TSK_02,_TASKS,US_06,Maine,truckDelivery,,450,4750,US_06_01_TSK_02_DESC,TASKS\nUS_06_01_ROCKS_02,US_06_01_ROCKS_02,_TASKS,US_06,Maine,cargoDelivery,2\u00d7 Crate Large,470,3900,US_06_01_ROCKS_02_DESC,TASKS\nUS_06_01_CON_02,US_06_01_CON_02,_CONTESTS,US_06,Maine,exploration,,120,1650,US_06_01_CON_02_DESC,CONTESTS\nUS_06_01_MILL_03_TRAILER,US_06_01_MILL_03_TRAILER,_CONTRACTS,US_06,Maine,truckDelivery,,600,7050,US_06_01_MILL_03_TRAILER_DESC,CONTRACTS\nUS_06_01_LOGGING_03,US_06_01_LOGGING_03,_CONTRACTS,US_06,Maine,cargoDelivery,1\u00d7 Wooden Planks Medium; 2\u00d7 Logs Medium; 2\u00d7 Wooden Planks Medium; 1\u00d7 Logs Long; 2\u00d7 Wooden Planks Long; 2\u00d7 Logs Short,2570,26500,US_06_01_LOGGING_03_DESC,CONTRACTS\nUS_06_01_GARAGE_STAGE_03_TRAILERS,US_06_01_GARAGE_STAGE_03_TRAILERS,_CONTRACTS,US_06,Maine,cargoDelivery,2\u00d7 Bags; 2\u00d7 Crate Large,1040,9800,US_06_01_GARAGE_STAGE_03_TRAILERS_DESC,CONTRACTS\nUS_06_01_TSK_03,US_06_01_TSK_03,_TASKS,US_06,Maine,cargoDelivery,5\u00d7 Radioctive,1110,14200,US_06_01_TSK_03_DESC,TASKS\nUS_06_01_ROCKS_03,US_06_01_ROCKS_03,_TASKS,US_06,Maine,cargoDelivery,1\u00d7 Wooden Planks Long,180,2250,US_06_01_ROCKS_03_DESC,TASKS\nUS_06_01_MILL_04_BUILD,US_06_01_MILL_04_BUILD,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Crate Large; 4\u00d7 Metal Roll,1410,11400,US_06_01_MILL_04_BUILD_DESC,CONTRACTS\nUS_06_01_TSK_04,US_06_01_TSK_04,_TASKS,US_06,Maine,truckDelivery,,510,5550,US_06_01_TSK_04_DESC,TASKS\nUS_06_01_TSK_05,US_06_01_TSK_05,_TASKS,US_06,Maine,truckDelivery,2\u00d7 Bags 2,370,3800,US_06_01_TSK_05_DESC,TASKS\nUS_06_01_TSK_06,US_06_01_TSK_06,_TASKS,US_06,Maine,cargoDelivery,5\u00d7 Cellulose,440,5200,US_06_01_TSK_06_DESC,TASKS\nUS_06_01_TSK_07,US_06_01_TSK_07,_TASKS,US_06,Maine,truckDelivery,5\u00d7 Bricks,560,5500,US_06_01_TSK_07_DESC,TASKS\nUS_06_01_TSK_08,US_06_01_TSK_08,_TASKS,US_06,Maine,truckDelivery,5\u00d7 Wooden Planks,480,5250,US_06_01_TSK_08_DESC,TASKS\nUS_06_01_TSK_09,US_06_01_TSK_09,_TASKS,US_06,Maine,truckDelivery,,580,6650,US_06_01_TSK_09_DESC,TASKS\nUS_06_01_TSK_10,US_06_01_TSK_10,_TASKS,US_06,Maine,truckDelivery,2\u00d7 Crate Large,460,4300,US_06_01_TSK_10_DESC,TASKS\nUS_06_01_TSK_11,US_06_01_TSK_11,_TASKS,US_06,Maine,exploration,,220,1580,US_06_01_TSK_11_DESC,TASKS\nUS_06_01_GAS_CONT,US_06_01_GAS_CONT,_CONTRACTS,US_06,Maine,truckDelivery,,350,3050,US_06_01_GAS_CONT_DESC,CONTRACTS\nUS_06_01_MILL_LOGS,US_06_01_MILL_LOGS,_CONTRACTS,US_06,Maine,cargoDelivery,1\u00d7 Logs Long; 2\u00d7 Logs Medium; 2\u00d7 Logs Short,2160,23850,US_06_01_MILL_LOGS_DESC,CONTRACTS\nUS_06_01_LOGGING_PIER,US_06_01_LOGGING_PIER,_CONTRACTS,US_06,Maine,cargoDelivery,2\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks Long,600,8800,US_06_01_LOGGING_PIER_DESC,CONTRACTS\nUS_06_02_BROKEN_WOOD_BRIDGE_1_TSK,US_06_02_BROKEN_WOOD_BRIDGE_1_TSK,_TASKS,US_06,Maine,cargoDelivery,4\u00d7 Wooden Planks; 4\u00d7 Wooden Planks,980,8500,US_06_02_BROKEN_WOOD_BRIDGE_1_DESC,TASKS\nUS_06_02_BROKEN_WOOD_BRIDGE_2_TSK,US_06_02_BROKEN_WOOD_BRIDGE_2_TSK,_TASKS,US_06,Maine,cargoDelivery,4\u00d7 Wooden Planks,670,8050,US_06_02_BROKEN_WOOD_BRIDGE_2_DESC,TASKS\nUS_06_02_EXIT_C,US_06_02_EXIT_C,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Metal Planks; 2\u00d7 Concrete Slab,1450,18300,US_06_02_EXIT_C_DESC,CONTRACTS\nUS_06_02_EXIT_B,US_06_02_EXIT_B,_CONTRACTS,US_06,Maine,cargoDelivery,4\u00d7 Metal Planks; 4\u00d7 Blocks,1150,16500,US_06_02_EXIT_B_DESC,CONTRACTS\nUS_06_02_MOTEL_SUPPLY,US_06_02_MOTEL_SUPPLY,_CONTRACTS,US_06,Maine,cargoDelivery,2\u00d7 Logs Short; 1\u00d7 Logs Medium; 3\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks Long; 3\u00d7 Service Spare Parts; 3\u00d7 Crate Large; 1\u00d7 Container Small,1500,16200,US_06_02_MOTEL_SUPPLY_DESC,CONTRACTS\nUS_06_02_ICEWATCH,US_06_02_ICEWATCH,_CONTRACTS,US_06,Maine,cargoDelivery,3\u00d7 Logs Medium; 4\u00d7 Wooden Planks; 2\u00d7 Service Spare Parts; 2\u00d7 Barrels; 2\u00d7 Vehicles Spare Parts,1100,12100,US_06_02_ICEWATCH_DESC,CONTRACTS\nUS_06_02_BROKENTRUCK_TSK,US_06_02_BROKENTRUCK_TSK,_TASKS,US_06,Maine,truckDelivery,,1030,9200,US_06_02_BROKENTRUCK_DESC,TASKS\nUS_06_02_CABINSUPPLY_TSK,US_06_02_CABINSUPPLY_TSK,_TASKS,US_06,Maine,truckDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts,1650,8700,US_06_02_CABINSUPPLY_DESC,TASKS\nUS_06_02_CABINS_TSK,US_06_02_CABINS_TSK,_TASKS,US_06,Maine,truckDelivery,,600,8400,US_06_02_CABINS_DESC,TASKS\nUS_06_02_OILTRUCK_TSK,US_06_02_OILTRUCK_TSK,_TASKS,US_06,Maine,truckDelivery,,780,7900,US_06_02_OILTRUCK_DESC,TASKS\nUS_06_02_FUELSTATION_TSK,US_06_02_FUELSTATION_TSK,_TASKS,US_06,Maine,truckDelivery,,880,7800,US_06_02_FUELSTATION_DESC,TASKS\nUS_06_02_TURNED_SCOUT_TSK,US_06_02_TURNED_SCOUT_TSK,_TASKS,US_06,Maine,truckDelivery,2\u00d7 Barrels Oil,1060,7100,US_06_02_TURNED_SCOUT_DESC,TASKS\nUS_06_02_FIREWORKS_TSK,US_06_02_FIREWORKS_TSK,_TASKS,US_06,Maine,truckDelivery,,600,6800,US_06_02_FIREWORKS_DESC,TASKS\nUS_06_02_LOADSTAR_TSK,US_06_02_LOADSTAR_TSK,_TASKS,US_06,Maine,truckDelivery,,430,4900,US_06_02_LOADSTAR_DESC,TASKS\nUS_06_02_TRAILER_TSK,US_06_02_TRAILER_TSK,_TASKS,US_06,Maine,truckDelivery,,400,3900,US_06_02_TRAILER_DESC,TASKS\nUS_06_02_BROKEN_BRIDGE_TSK,US_06_02_BROKEN_BRIDGE_TSK,_TASKS,US_06,Maine,cargoDelivery,3\u00d7 Concrete Slab; 3\u00d7 Metal Planks,1900,11000,US_06_02_BROKEN_BRIDGE_DESC,TASKS\nUS_06_02_BROKEN_POWERLINE_TSK,US_06_02_BROKEN_POWERLINE_TSK,_TASKS,US_06,Maine,cargoDelivery,3\u00d7 Metal Planks,980,8250,US_06_02_BROKEN_POWERLINE_DESC,TASKS\nUS_06_02_DROWNED_CARGO_TSK,US_06_02_DROWNED_CARGO_TSK,_TASKS,US_06,Maine,cargoDelivery,3\u00d7 Crate Large,780,6450,US_06_02_DROWNED_CARGO_DESC,TASKS\nUS_06_02_CHECKPLACES_TSK,US_06_02_CHECKPLACES_TSK,_TASKS,US_06,Maine,exploration,,160,2650,US_06_02_CHECKPLACES_DESC,TASKS\nUS_06_02_KARTER_CNT,US_06_02_KARTER_CNT,_CONTESTS,US_06,Maine,truckDelivery,,110,1560,US_06_02_KARTER_DESC,CONTESTS\nUS_06_02_DANGERRACE_CNT,US_06_02_DANGERRACE_CNT,_CONTESTS,US_06,Maine,exploration,,80,1250,US_06_02_DANGERRACE_DESC,CONTESTS\nUS_07_01_DELIVER_RIDER_1,US_07_01_DELIVER_RIDER_1,_CONTRACTS,US_07,Tennessee,truckDelivery,,1240,11350,US_07_01_DELIVER_RIDER_DESC,CONTRACTS\nUS_07_01_QUALIFICATION_01,US_07_01_QUALIFICATION_01,_CONTRACTS,US_07,Tennessee,exploration,,350,3500,US_07_01_QUALIFICATION_01_DESC,CONTRACTS\nUS_07_01_CIRCUIT_RACE_1,US_07_01_CIRCUIT_RACE_1,_CONTRACTS,US_07,Tennessee,exploration,,360,1400,US_07_01_CIRCUIT_RACE_1_DESC,CONTRACTS\nUS_07_01_DELIVER_RACING_01,US_07_01_DELIVER_RACING_01,_TASKS,US_07,Tennessee,truckDelivery,,540,6100,US_07_01_DELIVER_RACING_01_DESC,TASKS\nUS_07_01_DELIVER_BUGGY_01,US_07_01_DELIVER_BUGGY_01,_TASKS,US_07,Tennessee,truckDelivery,,420,4250,US_07_01_DELIVER_BUGGY_01_DESC,TASKS\nUS_07_01_RESTORE_TECH_ZONE_01,US_07_01_RESTORE_TECH_ZONE_01,_TASKS,US_07,Tennessee,cargoDelivery,4\u00d7 Vehicles Spare Parts,640,6600,US_07_01_RESTORE_TECH_ZONE_01_DESC,TASKS\nUS_07_01_ROCK_01,US_07_01_ROCK_01,_TASKS,US_07,Tennessee,cargoDelivery,2\u00d7 Wooden Planks Medium,180,1800,US_07_01_ROCK_01_DESC,TASKS\nUS_07_01_QUALIFICATION_01_START_SOLO,US_07_01_QUALIFICATION_01_START_SOLO,_CONTESTS,US_07,Tennessee,exploration,,150,1500,US_07_01_QUALIFICATION_DESC,CONTESTS\nUS_07_01_EXTREME_DESCENT_01,US_07_01_EXTREME_DESCENT_01,_CONTESTS,US_07,Tennessee,exploration,,100,450,US_07_01_EXTREME_DESCENT_01_DESC,CONTESTS\nUS_07_01_ROCK_02,US_07_01_ROCK_02,_TASKS,US_07,Tennessee,cargoDelivery,1\u00d7 Metal Planks,620,6600,US_07_01_ROCK_02_DESC,TASKS\nUS_07_01_RESTORE_TECH_ZONE_03,US_07_01_RESTORE_TECH_ZONE_03,_TASKS,US_07,Tennessee,cargoDelivery,4\u00d7 Vehicles Spare Parts,480,4250,US_07_01_RESTORE_TECH_ZONE_03_DESC,TASKS\nUS_07_01_BRIDGE_RESTORE_03,US_07_01_BRIDGE_RESTORE_03,_TASKS,US_07,Tennessee,cargoDelivery,2\u00d7 Wooden Planks Medium,320,3750,US_07_01_BRIDGE_RESTORE_03_DESC,TASKS\nUS_07_01_EVACUATION,US_07_01_EVACUATION,_CONTRACTS,US_07,Tennessee,truckDelivery,,1380,14150,US_07_01_EVACUATION_DESC,CONTRACTS\nUS_07_01_CONNECTION,US_07_01_CONNECTION,_CONTRACTS,US_07,Tennessee,truckDelivery,4\u00d7 Metal Planks,1770,11740,US_07_01_CONNECTION_DESC,CONTRACTS\nUS_07_01_RENEWAL_ELECTRICITY,US_07_01_RENEWAL_ELECTRICITY,_CONTRACTS,US_07,Tennessee,truckDelivery,4\u00d7 Barrels,460,4600,US_07_01_RENEWAL_ELECTRICITY_DESC,CONTRACTS\nUS_07_01_CAREER_SCOUT,US_07_01_CAREER_SCOUT,_CONTRACTS,US_07,Tennessee,truckDelivery,,560,4450,US_07_01_CAREER_SCOUT_DESC,CONTRACTS\nUS_07_01_DELIVERY_SPECIAL,US_07_01_DELIVERY_SPECIAL,_CONTRACTS,US_07,Tennessee,truckDelivery,4\u00d7 Service Spare Parts Special,420,3800,US_07_01_DELIVERY_SPECIAL_DESC,CONTRACTS\nUS_07_01_RESTORATION_SOLAR_STATION,US_07_01_RESTORATION_SOLAR_STATION,_CONTRACTS,US_07,Tennessee,cargoDelivery,2\u00d7 Metal Planks; 4\u00d7 Solar Panel,950,7500,US_07_01_RESTORATION_SOLAR_STATION_DESC,CONTRACTS\nUS_07_01_TRACK_RESTORATION,US_07_01_TRACK_RESTORATION,_CONTRACTS,US_07,Tennessee,cargoDelivery,3\u00d7 Wooden Planks Medium,570,7350,US_07_01_TRACK_RESTORATION_DESC,CONTRACTS\nUS_07_01_PREPARATION_TRAILS,US_07_01_PREPARATION_TRAILS,_CONTRACTS,US_07,Tennessee,cargoDelivery,2\u00d7 Crate Large,320,4200,US_07_01_PREPARATION_TRAILS_DESC,CONTRACTS\nUS_07_01_RACE_HEROES,US_07_01_RACE_HEROES,_CONTRACTS,US_07,Tennessee,exploration,,1140,19850,US_07_01_RACE_HEROES_DESC,CONTRACTS\nUS_07_01_ORIENTEERING,US_07_01_ORIENTEERING,_CONTRACTS,US_07,Tennessee,exploration,,1500,9000,US_07_01_ORIENTEERING_DESC,CONTRACTS\nUS_07_01_EXTREME_DESCENT,US_07_01_EXTREME_DESCENT,_CONTRACTS,US_07,Tennessee,exploration,,200,1000,US_07_01_EXTREME_DESCENT_DESC,CONTRACTS\nUS_07_01_EXTREME_SLOPE,US_07_01_EXTREME_SLOPE,_CONTRACTS,US_07,Tennessee,exploration,,100,950,US_07_01_EXTREME_SLOPE_DESC,CONTRACTS\nUS_07_01_CAREER,US_07_01_CAREER,_CONTRACTS,US_07,Tennessee,exploration,,50,500,US_07_01_CAREER_DESC,CONTRACTS\nUS_07_01_CAREER_START_HARD,US_07_01_CAREER_START_HARD,_TASKS,US_07,Tennessee,exploration,,200,1500,US_07_01_CAREER_START_HARD_DESC,TASKS\nUS_07_01_CAREER_START_MEDIUM,US_07_01_CAREER_START_MEDIUM,_TASKS,US_07,Tennessee,exploration,,100,1000,US_07_01_CAREER_START_MEDIUM_DESC,TASKS\nUS_07_01_CAREER_START_LIGHT,US_07_01_CAREER_START_LIGHT,_TASKS,US_07,Tennessee,exploration,,50,500,US_07_01_CAREER_START_LIGHT_DESC,TASKS\nUS_07_01_CIRCUIT_RACE,US_07_01_CIRCUIT_RACE,_CONTESTS,US_07,Tennessee,exploration,,350,1000,US_07_01_CIRCUIT_RACE_DESC,CONTESTS\nUS_07_01_QUALIFICATION,US_07_01_QUALIFICATION,_CONTESTS,US_07,Tennessee,exploration,,250,1000,US_07_01_QUALIFICATION_DESC,CONTESTS\nUS_07_01_EXTREME_SLOPE_SOLO,US_07_01_EXTREME_SLOPE_SOLO,_CONTESTS,US_07,Tennessee,exploration,,200,700,US_07_01_EXTREME_SLOPE_DESC,CONTESTS\nUS_07_01_RACE_START_SOLO,US_07_01_RACE_START_SOLO,_CONTESTS,US_07,Tennessee,exploration,,100,600,US_07_01_CIRCUIT_RACE_1_DESC,CONTESTS\nUS_07_01_EXTREME_DESCENT_START_SOLO,US_07_01_EXTREME_DESCENT_START_SOLO,_CONTESTS,US_07,Tennessee,exploration,,60,300,US_07_01_EXTREME_DESCENT_DESC,CONTESTS\nUS_07_01_PARKOUR,US_07_01_PARKOUR,_CONTESTS,US_07,Tennessee,exploration,,50,100,US_07_01_PARKOUR_DESC,CONTESTS\nUS_07_01_WORKOUT,US_07_01_WORKOUT,_CONTESTS,US_07,Tennessee,exploration,,10,20,US_07_01_WORKOUT_DESC,CONTESTS\nRU_08_01_FARMING_FIELD_1,RU_08_01_FARMING_FIELD_1,_TASKS,RU_08,Glades,exploration,,370,5800,RU_08_01_FARMING_FIELD_1_DESC,TASKS\nRU_08_01_INF_FARMING_1,RU_08_01_INF_FARMING_1,_TASKS,RU_08,Glades,exploration,,310,50,RU_08_01_INF_FARMING_1_DESC,TASKS\nRU_08_01_HAYSTACKS_2,RU_08_01_HAYSTACKS_2,_TASKS,RU_08,Glades,cargoDelivery,6\u00d7 Stack,370,3400,RU_08_01_HAYSTACKS_2_DESC,TASKS\nRU_08_01_FARMING_FIELD_2,RU_08_01_FARMING_FIELD_2,_TASKS,RU_08,Glades,exploration,,470,6400,RU_08_01_FARMING_FIELD_2_DESC,TASKS\nRU_08_01_WIND_POWER,RU_08_01_WIND_POWER,_CONTRACTS,RU_08,Glades,truckDelivery,1\u00d7 Logs Medium; 2\u00d7 Bags; 4\u00d7 Bags 2,2100,19800,RU_08_01_WIND_POWER_DESC,CONTRACTS\nRU_08_01_ELEVATOR_STUFF,RU_08_01_ELEVATOR_STUFF,_CONTRACTS,RU_08,Glades,truckDelivery,4\u00d7 Service Spare Parts,870,9300,RU_08_01_ELEVATOR_STUFF_DESC,CONTRACTS\nRU_08_01_FARMING_FIELD_TUTORIAL,RU_08_01_FARMING_FIELD_TUTORIAL,_CONTRACTS,RU_08,Glades,truckDelivery,,950,9200,RU_08_01_FARMING_FIELD_TUTORIAL_DECS,CONTRACTS\nRU_08_01_HELP_RAILWAY_WORKERS,RU_08_01_HELP_RAILWAY_WORKERS,_CONTRACTS,RU_08,Glades,truckDelivery,,430,4800,RU_08_01_HELP_RAILWAY_WORKERS_DESC,CONTRACTS\nRU_08_01_KIROVEC,RU_08_01_KIROVEC,_CONTRACTS,RU_08,Glades,truckDelivery,,250,2000,RU_08_01_KIROVEC_DESC,CONTRACTS\nRU_08_01_FACTORY_RECYCLE,RU_08_01_FACTORY_RECYCLE,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Barrels; 2\u00d7 Barrels Chemicals; 2\u00d7 Metal Planks,1270,12250,RU_08_01_FACTORY_RECYCLE_DESC,CONTRACTS\nRU_08_01_METALSCOUT,RU_08_01_METALSCOUT,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Pipes Small; 1\u00d7 Container Large,950,9200,RU_08_01_METALSCOUT_DESC,CONTRACTS\nRU_08_01_THROW_OUT_TRASH,RU_08_01_THROW_OUT_TRASH,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Rubbish,590,6650,RU_08_01_THROW_OUT_TRASH_DESC,CONTRACTS\nRU_08_01_HAZARD,RU_08_01_HAZARD,_CONTRACTS,RU_08,Glades,cargoDelivery,4\u00d7 Radioctive,550,4500,RU_08_01_HAZARD_DESC,CONTRACTS\nRU_08_01_VILLAGE_SUPPLIES,RU_08_01_VILLAGE_SUPPLIES,_TASKS,RU_08,Glades,truckDelivery,4\u00d7 Crate Large,660,6400,RU_08_01_VILLAGE_SUPPLIES_DESC,TASKS\nRU_08_01_EXPEDITION_HELP,RU_08_01_EXPEDITION_HELP,_TASKS,RU_08,Glades,truckDelivery,,540,6200,RU_08_01_EXPEDITION_HELP_DESC,TASKS\nRU_08_01_ILLEGAL_BUSINESS,RU_08_01_ILLEGAL_BUSINESS,_TASKS,RU_08,Glades,truckDelivery,,440,4550,RU_08_01_ILLEGAL_BUSINESS_DESC,TASKS\nRU_08_01_SUPPLIES_FOR_WATCHERS,RU_08_01_SUPPLIES_FOR_WATCHERS,_TASKS,RU_08,Glades,truckDelivery,,250,2500,RU_08_01_SUPPLIES_FOR_WATCHERS_DESC,TASKS\nRU_08_01_LOST_EVACUATION,RU_08_01_LOST_EVACUATION,_TASKS,RU_08,Glades,truckDelivery,,240,1800,RU_08_01_LOST_EVACUATION_DESC,TASKS\nRU_08_01_ECOLOGICAL_CHANGE,RU_08_01_ECOLOGICAL_CHANGE,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Container Small Special; 1\u00d7 Container Large Drilling,730,6660,RU_08_01_ECOLOGICAL_CHANGE_DESC,TASKS\nRU_08_01_GRAIN_ELEVATOR,RU_08_01_GRAIN_ELEVATOR,_TASKS,RU_08,Glades,cargoDelivery,4\u00d7 Crate Large,620,6350,RU_08_01_GRAIN_ELEVATOR_DESC,TASKS\nRU_08_01_VILLAGE_RENOVATION,RU_08_01_VILLAGE_RENOVATION,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Logs Medium; 2\u00d7 Bags 2,420,4650,RU_08_01_VILLAGE_RENOVATION_DESC,TASKS\nRU_08_01_FARM_VEHICLE_PARTS,RU_08_01_FARM_VEHICLE_PARTS,_TASKS,RU_08,Glades,cargoDelivery,3\u00d7 Vehicles Spare Parts,380,4450,RU_08_01_FARM_VEHICLE_PARTS_DESC,TASKS\nRU_08_01_POTATO_SELL,RU_08_01_POTATO_SELL,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Potato,50,3500,RU_08_01_POTATO_SELL_DESC,TASKS\nRU_08_01_PLANKS_SUPPLIES,RU_08_01_PLANKS_SUPPLIES,_TASKS,RU_08,Glades,cargoDelivery,4\u00d7 Wooden Planks,200,2250,RU_08_01_PLANKS_SUPPLIES_DESC,TASKS\nRU_08_01_DANGEROUS_BARRELS,RU_08_01_DANGEROUS_BARRELS,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Barrels Chemicals,270,2000,RU_08_01_DANGEROUS_BARRELS_DESC,TASKS\nRU_08_01_HAYSTACKS,RU_08_01_HAYSTACKS,_TASKS,RU_08,Glades,cargoDelivery,4\u00d7 Stack,100,1700,RU_08_01_HAYSTACKS_DESC,TASKS\nRU_08_01_SILLY_WORKERS,RU_08_01_SILLY_WORKERS,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Service Spare Parts,200,1500,RU_08_01_SILLY_WORKERS_DESC,TASKS\nRU_08_01_PROMO_PICS,RU_08_01_PROMO_PICS,_TASKS,RU_08,Glades,exploration,,200,2100,RU_08_01_PROMO_PICS_DESC,TASKS\nRU_08_01_CHECKING_TOURISTS,RU_08_01_CHECKING_TOURISTS,_TASKS,RU_08,Glades,exploration,,150,1000,RU_08_01_CHECKING_TOURISTS_DESC,TASKS\nRU_08_01_LAKE_RACE,RU_08_01_LAKE_RACE,_CONTESTS,RU_08,Glades,exploration,,260,260,RU_08_01_LAKE_RACE_DESC,CONTESTS\nRU_08_01_LOCAL_FUN,RU_08_01_LOCAL_FUN,_CONTESTS,RU_08,Glades,exploration,,150,150,RU_08_01_LOCAL_FUN_DESC,CONTESTS\nRU_08_02_JUNK_DELIVERY_01_CONTR,RU_08_02_JUNK_DELIVERY_01_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Crate Large; 2\u00d7 Bricks,630,6850,RU_08_02_JUNK_DELIVERY_01_DESC,CONTRACTS\nRU_08_02_BRIDGE_FIX_01,RU_08_02_BRIDGE_FIX_01,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Blocks,710,6800,RU_08_02_BRIDGE_FIX_DESC,TASKS\nRU_08_02_BRIDGE_LOGS_FIX_01,RU_08_02_BRIDGE_LOGS_FIX_01,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Logs Medium,260,2400,RU_08_02_BRIDGE_LOGS_FIX_DESC,TASKS\nRU_08_02_JUNK_DELIVERY_02_CONTR,RU_08_02_JUNK_DELIVERY_02_CONTR,_CONTRACTS,RU_08,Glades,truckDelivery,3\u00d7 Metal Roll,390,5700,RU_08_02_JUNK_DELIVERY_02_DESC,CONTRACTS\nRU_08_02_JUNK_DELIVERY_03,RU_08_02_JUNK_DELIVERY_03,_CONTRACTS,RU_08,Glades,cargoDelivery,3\u00d7 Concrete Slab,1200,13700,RU_08_02_JUNK_DELIVERY_03_DESC,CONTRACTS\nRU_08_02_WATER_TUBING,RU_08_02_WATER_TUBING,_CONTRACTS,RU_08,Glades,truckDelivery,1\u00d7 Tank Wagon; 1\u00d7 Tank Wagon,740,9200,RU_08_02_WATER_TUBING_DESC,CONTRACTS\nRU_08_02_VEHICAL_PARTS_HELP,RU_08_02_VEHICAL_PARTS_HELP,_CONTRACTS,RU_08,Glades,truckDelivery,3\u00d7 Vehicles Spare Parts,810,7750,RU_08_02_VEHICAL_PARTS_HELP_DESC,CONTRACTS\nRU_08_02_CHEMICAL_RESTORER,RU_08_02_CHEMICAL_RESTORER,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Radioctive; 1\u00d7 Radioctive; 1\u00d7 Radioctive; 1\u00d7 Radioctive; 1\u00d7 Radioctive,890,11350,RU_08_02_CHEMICAL_RESTORER_DESC,CONTRACTS\nRU_08_02_DRAIN_DRY,RU_08_02_DRAIN_DRY,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 1\u00d7 Forklift Caravan Container 2,980,10850,RU_08_02_DRAIN_DRY_DESC,CONTRACTS\nRU_08_02_CHEMICAL_BARRELS,RU_08_02_CHEMICAL_BARRELS,_CONTRACTS,RU_08,Glades,cargoDelivery,3\u00d7 Barrels Chemicals,650,8200,RU_08_02_CHEMICAL_BARRELS_DESC,CONTRACTS\nRU_08_02_FARMING,RU_08_02_FARMING,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Potato Gmo,710,7900,RU_08_02_FARMING_DESC,CONTRACTS\nRU_08_02_MANURE_TANKS,RU_08_02_MANURE_TANKS,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Tank Wagon,480,7250,RU_08_02_MANURE_TANKS_DESC,CONTRACTS\nRU_08_02_GMO_POTATO_BUYING,RU_08_02_GMO_POTATO_BUYING,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Potato Gmo,60,6000,RU_08_02_GMO_POTATO_BUYING_DESC,CONTRACTS\nRU_08_02_POWER_FIX,RU_08_02_POWER_FIX,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Crate Large,660,5900,RU_08_02_POWER_FIX_DESC,CONTRACTS\nRU_08_02_TECH_FIX_CONTR,RU_08_02_TECH_FIX_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Barrels Oil; 2\u00d7 Vehicles Spare Parts,440,4600,RU_08_02_TECH_FIX_DESC,CONTRACTS\nRU_08_02_SEISMO_SCOUT,RU_08_02_SEISMO_SCOUT,_CONTRACTS,RU_08,Glades,exploration,,300,5150,RU_08_02_SEISMO_SCOUT_DESC,CONTRACTS\nRU_08_02_SCIENCE_SCOUT,RU_08_02_SCIENCE_SCOUT,_CONTRACTS,RU_08,Glades,exploration,,230,3900,RU_08_02_SCIENCE_SCOUT_DESC,CONTRACTS\nRU_08_02_HARVESTER_DELIVERY,RU_08_02_HARVESTER_DELIVERY,_TASKS,RU_08,Glades,truckDelivery,,300,3600,RU_08_02_HARVESTER_DELIVERY_DESC,TASKS\nRU_08_02_POSTMAN_CAR_DELIVERY_TASK,RU_08_02_POSTMAN_CAR_DELIVERY_TASK,_TASKS,RU_08,Glades,truckDelivery,,260,2550,RU_08_02_POSTMAN_CAR_DELIVERY_DESC,TASKS\nRU_08_02_CONSUMABLE_MATERIAL,RU_08_02_CONSUMABLE_MATERIAL,_TASKS,RU_08,Glades,cargoDelivery,3\u00d7 Crate Large,390,3900,RU_08_02_CONSUMABLE_MATERIAL_DESC,TASKS\nRU_08_02_LOST_CARGO,RU_08_02_LOST_CARGO,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Wooden Planks,260,3400,RU_08_02_LOST_CARGO_DESC,TASKS\nRU_08_02_DELIVERY_TASK,RU_08_02_DELIVERY_TASK,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Container Large,340,2800,RU_08_02_DELIVERY_TASK_DESC,TASKS\nRU_08_02_FOR_REPAIR,RU_08_02_FOR_REPAIR,_TASKS,RU_08,Glades,exploration,,150,1300,RU_08_02_FOR_REPAIR_DESC,TASKS\nRU_08_02_ENDLESS_FARMING,RU_08_02_ENDLESS_FARMING,_TASKS,RU_08,Glades,exploration,,530,50,RU_08_02_ENDLESS_FARMING_DESC,TASKS\nRU_08_02_DANGEROUS_DELIVERY,RU_08_02_DANGEROUS_DELIVERY,_CONTESTS,RU_08,Glades,cargoDelivery,1\u00d7 Container Small,100,800,RU_08_02_DANGEROUS_DELIVERY_DESC,CONTESTS\nRU_08_02_SWAMP_RACE,RU_08_02_SWAMP_RACE,_CONTESTS,RU_08,Glades,exploration,,50,500,RU_08_02_SWAMP_RACE_DESC,CONTESTS\nRU_08_02_DOWNHILL_RACE,RU_08_02_DOWNHILL_RACE,_CONTESTS,RU_08,Glades,exploration,,100,300,RU_08_02_DOWNHILL_RACE_DESC,CONTESTS\nRU_08_03_SOLAR_STATION_PANEL_01_CONTR,RU_08_03_SOLAR_STATION_PANEL_01_CONTR,_CONTRACTS,RU_08,Glades,truckDelivery,3\u00d7 Solar Panel,700,6950,RU_08_03_SOLAR_STATION_PANEL_01_DESC,CONTRACTS\nRU_08_03_FARMING_TASK_01,RU_08_03_FARMING_TASK_01,_TASKS,RU_08,Glades,exploration,,550,50,RU_08_03_FARMING_TASK_01_DESC,TASKS\nRU_08_03_SOLAR_STATION_PANEL_02_CONTR,RU_08_03_SOLAR_STATION_PANEL_02_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Solar Panel; 1\u00d7 Container Large,1000,9700,RU_08_03_SOLAR_STATION_PANEL_02_DESC,CONTRACTS\nRU_08_03_SOLAR_STATION_PANEL_03_CONTR,RU_08_03_SOLAR_STATION_PANEL_03_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,3\u00d7 Solar Panel; 2\u00d7 Metal Roll,880,9100,RU_08_03_SOLAR_STATION_PANEL_03_DESC,CONTRACTS\nRU_08_03_GENERATOR_DELIVERY_CONTR,RU_08_03_GENERATOR_DELIVERY_CONTR,_CONTRACTS,RU_08,Glades,truckDelivery,,220,2300,RU_08_03_GENERATOR_DELIVERY_DESC,CONTRACTS\nRU_08_03_GARBAGE_DELIVERY_CONTR,RU_08_03_GARBAGE_DELIVERY_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,4\u00d7 Rubbish,1430,15450,RU_08_03_GARBAGE_DELIVERY_DESC,CONTRACTS\nRU_08_03_TRANSFORMATOR_RESTORE,RU_08_03_TRANSFORMATOR_RESTORE,_CONTRACTS,RU_08,Glades,cargoDelivery,4\u00d7 Metal Planks; 1\u00d7 Container Small Special,980,8600,RU_08_03_TRANSFORMATOR_RESTORE_DESC,CONTRACTS\nRU_08_03_FARMING,RU_08_03_FARMING,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Potato; 1\u00d7 Potato,730,7900,RU_08_03_FARMING_DESC,CONTRACTS\nRU_08_03_ROAD_WAYS_FIX_CONTR,RU_08_03_ROAD_WAYS_FIX_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Railway,530,6000,RU_08_03_ROAD_WAYS_FIX_DESC,CONTRACTS\nRU_08_03_SOLAR_STATION_WIRING,RU_08_03_SOLAR_STATION_WIRING,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Medium,500,5150,RU_08_03_SOLAR_STATION_WIRING_DESC,CONTRACTS\nRU_08_03_RESYCLE_DELIVERY,RU_08_03_RESYCLE_DELIVERY,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts,410,4600,RU_08_03_RESYCLE_DELIVERY_DESC,CONTRACTS\nRU_08_03_COLLECTIVE_FARM_METALLDETECTING,RU_08_03_COLLECTIVE_FARM_METALLDETECTING,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Container Small Special,270,2750,RU_08_03_COLLECTIVE_FARM_METALLDETECTING_DESC,CONTRACTS\nRU_08_03_TRAILER_MATERIALS_DELIVERY,RU_08_03_TRAILER_MATERIALS_DELIVERY,_TASKS,RU_08,Glades,truckDelivery,,640,6650,RU_08_03_TRAILER_MATERIALS_DELIVERY_DESC,TASKS\nRU_08_03_HAUL_TRANSPORT,RU_08_03_HAUL_TRANSPORT,_TASKS,RU_08,Glades,truckDelivery,,600,6650,RU_08_03_HAUL_TRANSPORT_DESC,TASKS\nRU_08_03_PRODUCT_DELIVERY,RU_08_03_PRODUCT_DELIVERY,_TASKS,RU_08,Glades,truckDelivery,,460,5800,RU_08_03_PRODUCT_DELIVERY_DESC,TASKS\nRU_08_03_CARGO_DELIVERY_TRUCK,RU_08_03_CARGO_DELIVERY_TRUCK,_TASKS,RU_08,Glades,truckDelivery,,460,5700,RU_08_03_CARGO_DELIVERY_TRUCK_DESC,TASKS\nRU_08_03_TRANSPORT_EVACUATION,RU_08_03_TRANSPORT_EVACUATION,_TASKS,RU_08,Glades,truckDelivery,,420,5100,RU_08_03_TRANSPORT_EVACUATION_DESC,TASKS\nRU_08_03_POTATO_SELLING,RU_08_03_POTATO_SELLING,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Potato,50,5800,RU_08_03_POTATO_SELLING_DESC,TASKS\nRU_08_03_ROAD_BLOCKAGE,RU_08_03_ROAD_BLOCKAGE,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Service Spare Parts Special,310,3650,RU_08_03_ROAD_BLOCKAGE_DESC,TASKS\nRU_08_03_BROKEN_PIPES_RESTORE,RU_08_03_BROKEN_PIPES_RESTORE,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Pipes Medium; 1\u00d7 Pipes Medium,400,3550,RU_08_03_BROKEN_PIPES_RESTORE_DESC,TASKS\nRU_08_03_LOST_GARBAGE_DELIVERY,RU_08_03_LOST_GARBAGE_DELIVERY,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Rubbish,290,2750,RU_08_03_LOST_GARBAGE_DELIVERY_DESC,TASKS\nRU_08_03_HAY_DELIVERY,RU_08_03_HAY_DELIVERY,_TASKS,RU_08,Glades,cargoDelivery,3\u00d7 Stack,200,2250,RU_08_03_HAY_DELIVERY_DESC,TASKS\nRU_08_03_TV_TOWER_RESTORE,RU_08_03_TV_TOWER_RESTORE,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Metal Roll,240,2200,RU_08_03_TV_TOWER_RESTORE_DESC,TASKS\nRU_08_03_BARN_RESTORE,RU_08_03_BARN_RESTORE,_TASKS,RU_08,Glades,cargoDelivery,1\u00d7 Logs Long,240,2100,RU_08_03_BARN_RESTORE_DESC,TASKS\nRU_08_03_URGENT_DELIVERY_CARGO,RU_08_03_URGENT_DELIVERY_CARGO,_CONTESTS,RU_08,Glades,cargoDelivery,1\u00d7 Container Small,200,1500,RU_08_03_URGENT_DELIVERY_CARGO_DESC,CONTESTS\nRU_08_03_FIELDS_RACING,RU_08_03_FIELDS_RACING,_CONTESTS,RU_08,Glades,exploration,,150,150,RU_08_03_FIELDS_RACING_DESC,CONTESTS\nRU_08_04_CONNECTION_01,RU_08_04_CONNECTION_01,_CONTRACTS,RU_08,Glades,truckDelivery,,500,5750,RU_08_04_CONNECTION_01_DESC,CONTRACTS\nRU_08_04_COMPLEX_RECOVERY_01,RU_08_04_COMPLEX_RECOVERY_01,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Wooden Planks Medium; 2\u00d7 Metal Planks,490,3900,RU_08_04_COMPLEX_RECOVERY_01_DESC,CONTRACTS\nRU_08_04_SPECIAL_DELIVERY_01,RU_08_04_SPECIAL_DELIVERY_01,_TASKS,RU_08,Glades,truckDelivery,,420,4250,RU_08_04_SPECIAL_DELIVERY_01_DESC,TASKS\nRU_08_04_LOGS_01_A,RU_08_04_LOGS_01_a,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Logs Medium,520,5300,RU_08_04_LOGS_01_DESC,TASKS\nRU_08_04_FIELD_PROCESSING_01,RU_08_04_FIELD_PROCESSING_01,_TASKS,RU_08,Glades,exploration,,400,50,RU_08_04_FIELD_PROCESSING_01_DESC,TASKS\nRU_08_04_COMPLEX_RECOVERY_02_CONTR,RU_08_04_COMPLEX_RECOVERY_02_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Vehicles Spare Parts,1130,11200,RU_08_04_COMPLEX_RECOVERY_02_DESC,CONTRACTS\nRU_08_04_LOGS_02_A,RU_08_04_LOGS_02_a,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Logs Medium,510,5150,RU_08_04_LOGS_02_DESC,TASKS\nRU_08_04_BRIDGE_RESTORE_02,RU_08_04_BRIDGE_RESTORE_02,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Metal Planks,350,2450,RU_08_04_BRIDGE_RESTORE_DESC,TASKS\nRU_08_04_COMPLEX_RECOVERY_03,RU_08_04_COMPLEX_RECOVERY_03,_CONTRACTS,RU_08,Glades,cargoDelivery,2\u00d7 Wooden Planks Medium; 4\u00d7 Metal Roll,640,5500,RU_08_04_COMPLEX_RECOVERY_03_DESC,CONTRACTS\nRU_08_04_WINDMILL,RU_08_04_WINDMILL,_CONTRACTS,RU_08,Glades,truckDelivery,,1070,14000,RU_08_04_WINDMILL_DESC,CONTRACTS\nRU_08_04_LOST_TRUCKS,RU_08_04_LOST_TRUCKS,_CONTRACTS,RU_08,Glades,truckDelivery,,780,7300,RU_08_04_LOST_TRUCKS_DESC,CONTRACTS\nRU_08_04_GARAGE_RECOVERY_CONTR,RU_08_04_GARAGE_RECOVERY_CONTR,_CONTRACTS,RU_08,Glades,cargoDelivery,4\u00d7 Service Spare Parts; 2\u00d7 Metal Planks,2120,23800,RU_08_04_GARAGE_DESC,CONTRACTS\nRU_08_04_POTATO_BUYING_META,RU_08_04_POTATO_BUYING_META,_CONTRACTS,RU_08,Glades,cargoDelivery,8\u00d7 Potato,930,6700,RU_08_04_POTATO_BUYING_META_DESC,CONTRACTS\nRU_08_04_WIRE_PULLING,RU_08_04_WIRE_PULLING,_CONTRACTS,RU_08,Glades,cargoDelivery,1\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks Medium,540,5550,RU_08_04_WIRE_PULLING_DESC,CONTRACTS\nRU_08_04_AIRPORT_RECOVERY,RU_08_04_AIRPORT_RECOVERY,_CONTRACTS,RU_08,Glades,cargoDelivery,4\u00d7 Barrels Oil,680,5400,RU_08_04_AIRPORT_DESC,CONTRACTS\nRU_08_04_FISHING,RU_08_04_FISHING,_TASKS,RU_08,Glades,truckDelivery,,530,6250,RU_08_04_FISHING_DESC,TASKS\nRU_08_04_LOST_CARGO,RU_08_04_LOST_CARGO,_TASKS,RU_08,Glades,cargoDelivery,4\u00d7 Service Spare Parts,500,6000,RU_08_04_LOST_CARGO_DESC,TASKS\nRU_08_04_HAY_DELIVERY,RU_08_04_HAY_DELIVERY,_TASKS,RU_08,Glades,cargoDelivery,3\u00d7 Stack,400,4650,RU_08_04_HAY_DELIVERY_DESC,TASKS\nRU_08_04_POTATO_BUYING,RU_08_04_POTATO_BUYING,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Potato,50,3600,RU_08_04_POTATO_BUYING_DESC,TASKS\nRU_08_04_IMPORTANT_CARGO,RU_08_04_IMPORTANT_CARGO,_TASKS,RU_08,Glades,cargoDelivery,2\u00d7 Container Small,430,3300,RU_08_04_IMPORTANT_CARGO_DESC,TASKS\nRU_08_04_LAKE,RU_08_04_LAKE,_TASKS,RU_08,Glades,exploration,,460,8000,RU_08_04_LAKE_DESC,TASKS\nRU_08_04_CONTEST,RU_08_04_CONTEST,_CONTESTS,RU_08,Glades,exploration,,100,1200,RU_08_04_CONTEST_DESC,CONTESTS\nRU_08_04_RING,RU_08_04_RING,_CONTESTS,RU_08,Glades,exploration,,125,1000,RU_08_04_RING_DESC,CONTESTS\nUS_09_01_LOG_HOUSE_CONTRACT_01,US_09_01_LOG_HOUSE_CONTRACT_01,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Wooden Planks Medium; 1\u00d7 Logs Long; 1\u00d7 Wooden Planks Long; 2\u00d7 Logs Short,1500,17950,US_09_01_LOG_HOUSE_CONTRACT_01_DESC,CONTRACTS\nUS_09_01_WATER_TOWER_CONTRACT_01,US_09_01_WATER_TOWER_CONTRACT_01,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 2\u00d7 Metal Roll,500,4500,US_09_01_WATER_TOWER_CONTRACT_01_DESC,CONTRACTS\nUS_09_01_WATER_TOWER_TASK_01,US_09_01_WATER_TOWER_TASK_01,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Metal Roll; 1\u00d7 Pipes Small,500,4500,US_09_01_WATER_TOWER_TASK_01_DESC,TASKS\nUS_09_01_LOG_HOUSE_CONTRACT_02,US_09_01_LOG_HOUSE_CONTRACT_02,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Wooden Planks Long; 1\u00d7 Logs Medium; 1\u00d7 Wooden Planks Medium; 3\u00d7 Logs Short,1730,21800,US_09_01_LOG_HOUSE_CONTRACT_02_DESC,CONTRACTS\nUS_09_01_WATER_TOWER_CONTRACT_02,US_09_01_WATER_TOWER_CONTRACT_02,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 2\u00d7 Pipes Small,500,4500,US_09_01_WATER_TOWER_CONTRACT_02_DESC,CONTRACTS\nUS_09_01_WATER_TOWER_TASK_02,US_09_01_WATER_TOWER_TASK_02,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 1\u00d7 Metal Planks; 1\u00d7 Metal Roll,500,4500,US_09_01_WATER_TOWER_TASK_02_DESC,TASKS\nUS_09_01_WATER_TOWER_TASK_03,US_09_01_WATER_TOWER_TASK_03,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 1\u00d7 Metal Roll; 2\u00d7 Bags 2,500,4500,US_09_01_WATER_TOWER_TASK_03_DESC,TASKS\nUS_09_01_WATER_TOWER_TASK_04,US_09_01_WATER_TOWER_TASK_04,_TASKS,US_09,Ontario,cargoDelivery,2\u00d7 Bags 2; 2\u00d7 Pipes Small,500,4500,US_09_01_WATER_TOWER_TASK_04_DESC,TASKS\nUS_09_01_GOLD_TRUCK_DELIVERY_CONTRACT,US_09_01_GOLD_TRUCK_DELIVERY_CONTRACT,_CONTRACTS,US_09,Ontario,truckDelivery,,890,9700,US_09_01_GOLD_TRUCK_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_01_FIRE_STATION_VEHICLE_CONTRACT,US_09_01_FIRE_STATION_VEHICLE_CONTRACT,_CONTRACTS,US_09,Ontario,truckDelivery,,370,4200,US_09_01_FIRE_STATION_VEHICLE_CONTRACT_DESC,CONTRACTS\nUS_09_01_GOLD_DELIVERY_CONTRACT,US_09_01_GOLD_DELIVERY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,8\u00d7 Gold,2450,25200,US_09_01_GOLD_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_01_WATER_DELIVERY_RAILWAY_CONTRACT,US_09_01_WATER_DELIVERY_RAILWAY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,6400\u00d7 Water; 6400\u00d7 Water,2660,23400,US_09_01_WATER_DELIVERY_RAILWAY_CONTRACT_DESC,CONTRACTS\nUS_09_01_WATER_DELIVERY_WAREHOUSE_CONTRACT,US_09_01_WATER_DELIVERY_WAREHOUSE_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,4400\u00d7 Water; 4400\u00d7 Water; 4400\u00d7 Water,1890,14250,US_09_01_WATER_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_01_GARAGE_RESTORE_CONTRACT,US_09_01_GARAGE_RESTORE_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,2\u00d7 Metal Roll; 2\u00d7 Metal Planks; 2\u00d7 Bricks,1150,10550,US_09_01_GARAGE_RESTORE_CONTRACT_DESC,CONTRACTS\nUS_09_01_RAILWAY_DELIVERY_CONTRACT,US_09_01_RAILWAY_DELIVERY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Iron Road,1040,9100,US_09_01_RAILWAY_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_01_BURNED_LOGS_CLEARING_CONTRACT,US_09_01_BURNED_LOGS_CLEARING_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,4\u00d7 Burnt Logs,840,8400,US_09_01_BURNED_LOGS_CLEARING_CONTRACT_DESC,CONTRACTS\nUS_09_01_AFFECTED_INFROSTRUCTURE_SCOUT_CONTRACT,US_09_01_AFFECTED_INFROSTRUCTURE_SCOUT_CONTRACT,_CONTRACTS,US_09,Ontario,exploration,,120,2000,US_09_01_AFFECTED_INFROSTRUCTURE_SCOUT_CONTRACT_DESC,CONTRACTS\nUS_09_01_WORKER_SCOUT_EVACUATION_TASK,US_09_01_WORKER_SCOUT_EVACUATION_TASK,_TASKS,US_09,Ontario,truckDelivery,,440,5450,US_09_01_WORKER_SCOUT_EVACUATION_TASK_DESC,TASKS\nUS_09_01_BAGS_DELIVERY_TASK,US_09_01_BAGS_DELIVERY_TASK,_TASKS,US_09,Ontario,truckDelivery,2\u00d7 Bags,410,3950,US_09_01_BAGS_DELIVERY_TASK_DESC,TASKS\nUS_09_01_RANGER_SCOUT_EVACUATION_TASK,US_09_01_RANGER_SCOUT_EVACUATION_TASK,_TASKS,US_09,Ontario,truckDelivery,,410,3950,US_09_01_RANGER_SCOUT_EVACUATION_TASK_DESC,TASKS\nUS_09_01_LOST_RADIOACTIVE_DELIVERY_TASK,US_09_01_LOST_RADIOACTIVE_DELIVERY_TASK,_TASKS,US_09,Ontario,cargoDelivery,5\u00d7 Radioctive,1260,17700,US_09_01_LOST_RADIOACTIVE_DELIVERY_TASK_DESC,TASKS\nUS_09_01_LOST_BOAT_TASK,US_09_01_LOST_BOAT_TASK,_TASKS,US_09,Ontario,cargoDelivery,3\u00d7 Barrels Chemicals,800,9600,US_09_01_LOST_BOAT_TASK_DESC,TASKS\nUS_09_01_LOST_MECHANISM_TASK,US_09_01_LOST_MECHANISM_TASK,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Service Spare Parts Special; 1\u00d7 Service Spare Parts Special; 1\u00d7 Service Spare Parts Special,660,8100,US_09_01_LOST_MECHANISM_TASK_DESC,TASKS\nUS_09_01_SEEMAN_TASK,US_09_01_SEEMAN_TASK,_TASKS,US_09,Ontario,cargoDelivery,2\u00d7 Metal Roll; 2\u00d7 Bags 2,780,7700,US_09_01_SEEMAN_TASK_DESC,TASKS\nUS_09_01_TRAILERS_LOST_STUFF_TASK,US_09_01_TRAILERS_LOST_STUFF_TASK,_TASKS,US_09,Ontario,cargoDelivery,2\u00d7 Container Small,620,7150,US_09_01_TRAILERS_LOST_STUFF_TASK_DESC,TASKS\nUS_09_01_PORT_RESOURCES_DELIVERY_TASK,US_09_01_PORT_RESOURCES_DELIVERY_TASK,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 2\u00d7 Blocks,630,5550,US_09_01_PORT_RESOURCES_DELIVERY_TASK_DESC,TASKS\nUS_09_01_MINE_GARBAGE_TASK,US_09_01_MINE_GARBAGE_TASK,_TASKS,US_09,Ontario,cargoDelivery,2\u00d7 Rubbish,550,5300,US_09_01_MINE_GARBAGE_TASK_DESC,TASKS\nUS_09_01_BRIDGE_TASK,US_09_01_BRIDGE_TASK,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Blocks,450,4350,US_09_01_BRIDGE_TASK_DESC,TASKS\nUS_09_01_TOURIST_CAMPING_TASK,US_09_01_TOURIST_CAMPING_TASK,_TASKS,US_09,Ontario,exploration,,550,9400,US_09_01_TOURIST_CAMPING_TASK_DESC,TASKS\nUS_09_01_BARRELS_EXPRESS_DELIVERY_CONTEST,US_09_01_BARRELS_EXPRESS_DELIVERY_CONTEST,_CONTESTS,US_09,Ontario,cargoDelivery,1\u00d7 Barrels; 1\u00d7 Barrels,260,1500,US_09_01_BARRELS_EXPRESS_DELIVERY_CONTEST_DESC,CONTESTS\nUS_09_02_WATER_TOWER_CONTRACT_01,US_09_02_WATER_TOWER_CONTRACT_01,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Crate Large; 3\u00d7 Pipes Small,500,4500,US_09_02_WATER_TOWER_CONTRACT_01_DESC,CONTRACTS\nUS_09_02_VR_TASK_01,US_09_02_VR_TASK_01,_TASKS,US_09,Ontario,cargoDelivery,3\u00d7 Wooden Planks; 2\u00d7 Bags 2,610,6400,US_09_02_VR_TASK_01_DESC,TASKS\nUS_09_02_WATER_TOWER_TASK_01,US_09_02_WATER_TOWER_TASK_01,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 3\u00d7 Metal Roll,500,4500,US_09_02_WATER_TOWER_TASK_01_DESC,TASKS\nUS_09_02_WATER_TOWER_CONTRACT_02,US_09_02_WATER_TOWER_CONTRACT_02,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Crate Large; 2\u00d7 Metal Roll,500,4500,US_09_02_WATER_TOWER_CONTRACT_02_DESC,CONTRACTS\nUS_09_02_WATER_TOWER_TASK_02,US_09_02_WATER_TOWER_TASK_02,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Crate Large; 2\u00d7 Bags 2,500,4500,US_09_02_WATER_TOWER_TASK_02_DESC,TASKS\nUS_09_02_VR_TASK_02,US_09_02_VR_TASK_02,_TASKS,US_09,Ontario,cargoDelivery,4\u00d7 Vehicles Spare Parts,370,3900,US_09_02_VR_TASK_02_DESC,TASKS\nUS_09_02_VR_TASK_03,US_09_02_VR_TASK_03,_TASKS,US_09,Ontario,truckDelivery,,600,7850,US_09_02_VR_TASK_03_DESC,TASKS\nUS_09_02_WATER_TOWER_TASK_03,US_09_02_WATER_TOWER_TASK_03,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 1\u00d7 Metal Planks; 1\u00d7 Metal Roll,500,4500,US_09_02_WATER_TOWER_TASK_03_DESC,TASKS\nUS_09_02_WATER_DELIVERY_FACTORY_CONTRACT,US_09_02_WATER_DELIVERY_FACTORY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,5900\u00d7 Water; 5900\u00d7 Water; 5900\u00d7 Water,3750,30900,US_09_02_WATER_DELIVERY_FACTORY_CONTRACT_DESC,CONTRACTS\nUS_09_02_SEISMO_WATER_DELIVERY_CONTRACT,US_09_02_SEISMO_WATER_DELIVERY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,2200\u00d7 Water; 2\u00d7 Bags 2; 2200\u00d7 Water; 2\u00d7 Bags 2; 2200\u00d7 Water; 2\u00d7 Bags 2,2610,19800,US_09_02_SEISMO_WATER_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_02_FIRE_WATCHTOWER_CONTRACT,US_09_02_FIRE_WATCHTOWER_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Logs Short; 3\u00d7 Service Spare Parts; 2\u00d7 Barrels,1680,19500,US_09_02_FIRE_WATCHTOWER_CONTRACT_DESC,CONTRACTS\nUS_09_02_FUEL_FOR_STATION_DELIVERY_CONTRACT,US_09_02_FUEL_FOR_STATION_DELIVERY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,3\u00d7 Service Spare Parts; 2\u00d7 Barrels; 2\u00d7 Service Spare Parts; 1\u00d7 Metal Planks; 1\u00d7 Barrels,1410,14850,US_09_02_FUEL_FOR_STATION_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_09_02_UNCLEAR_GOLD_CONTRACT,US_09_02_UNCLEAR_GOLD_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,2\u00d7 Container Large,1470,13600,US_09_02_UNCLEAR_GOLD_CONTRACT_DESC,CONTRACTS\nUS_09_02_DISASSEMBLY_DRILL_CONTRACT,US_09_02_DISASSEMBLY_DRILL_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,1\u00d7 Container Large Drilling; 2\u00d7 Blocks; 2\u00d7 Wooden Planks,1360,12350,US_09_02_DISASSEMBLY_DRILL_CONTRACT_DESC,CONTRACTS\nUS_09_02_PIPES_CONTRACT,US_09_02_PIPES_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,2\u00d7 Pipes Medium; 2\u00d7 Pipes Medium,1110,10250,US_09_02_PIPES_CONTRACT_DESC,CONTRACTS\nUS_09_02_WATER_DELIVERY_WAREHOUSE_CONTRACT,US_09_02_WATER_DELIVERY_WAREHOUSE_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,4400\u00d7 Water; 4400\u00d7 Water,1260,9500,US_09_02_WATER_DELIVERY_WAREHOUSE_CONTRACT_DESC,CONTRACTS\nUS_09_02_EXIT_CONTRACT,US_09_02_EXIT_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,3\u00d7 Metal Planks; 3\u00d7 Blocks,860,7350,US_09_02_EXIT_CONTRACT_DESC,CONTRACTS\nUS_09_02_EXPORT_HAY_CONTRACT,US_09_02_EXPORT_HAY_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,6\u00d7 Stack,560,6350,US_09_02_EXPORT_HAY_CONTRACT_DESC,CONTRACTS\nUS_09_02_LUMBER_MILL_REPAIR_CONTRACT,US_09_02_LUMBER_MILL_REPAIR_CONTRACT,_CONTRACTS,US_09,Ontario,cargoDelivery,4\u00d7 Service Spare Parts; 1\u00d7 Forklift Caravan Container 2,600,5650,US_09_02_LUMBER_MILL_REPAIR_CONTRACT_DESC,CONTRACTS\nUS_09_02_SEISMO_SCOUT_CONTRACT,US_09_02_SEISMO_SCOUT_CONTRACT,_CONTRACTS,US_09,Ontario,exploration,,150,2250,US_09_02_SEISMO_SCOUT_CONTRACT_DESC,CONTRACTS\nUS_09_02_FORGOTTEN_TRAILER_DELIVERY_TASK,US_09_02_FORGOTTEN_TRAILER_DELIVERY_TASK,_TASKS,US_09,Ontario,truckDelivery,2\u00d7 Service Spare Parts,840,9900,US_09_02_FORGOTTEN_TRAILER_DELIVERY_TASK_DESC,TASKS\nUS_09_02_CRASH_TASK,US_09_02_CRASH_TASK,_TASKS,US_09,Ontario,truckDelivery,,620,5800,US_09_02_CRASH_TASK_DESC,TASKS\nUS_09_02_RENEGADE_SCOUT_DELIVERY_TASK,US_09_02_RENEGADE_SCOUT_DELIVERY_TASK,_TASKS,US_09,Ontario,truckDelivery,,340,3550,US_09_02_RENEGADE_SCOUT_DELIVERY_TASK_DESC,TASKS\nUS_09_02_TATRA_FARM_TASK,US_09_02_TATRA_FARM_TASK,_TASKS,US_09,Ontario,truckDelivery,,320,3300,US_09_02_TATRA_FARM_TASK_DESC,TASKS\nUS_09_02_DON_SCOUT_TASK,US_09_02_DON_SCOUT_TASK,_TASKS,US_09,Ontario,truckDelivery,,290,2700,US_09_02_DON_SCOUT_TASK_DESC,TASKS\nUS_09_02_LOGS_VEHICLE_TASK,US_09_02_LOGS_VEHICLE_TASK,_TASKS,US_09,Ontario,truckDelivery,,210,1800,US_09_02_LOGS_VEHICLE_TASK_DESC,TASKS\nUS_09_02_QUARRY_LOST_CARGO_TASK,US_09_02_QUARRY_LOST_CARGO_TASK,_TASKS,US_09,Ontario,cargoDelivery,3\u00d7 Concrete Slab,1010,8550,US_09_02_QUARRY_LOST_CARGO_TASK_DESC,TASKS\nUS_09_02_METALLDETECTING_TASK,US_09_02_METALLDETECTING_TASK,_TASKS,US_09,Ontario,cargoDelivery,1\u00d7 Container Small Special,580,7450,US_09_02_METALLDETECTING_TASK_DESC,TASKS\nUS_09_02_NEW_QUARRY_TASK,US_09_02_NEW_QUARRY_TASK,_TASKS,US_09,Ontario,exploration,,700,12100,US_09_02_NEW_QUARRY_TASK_DESC,TASKS\nUS_09_02_MOUNTAIN_TOURISM_CONTEST,US_09_02_MOUNTAIN_TOURISM_CONTEST,_CONTESTS,US_09,Ontario,exploration,,105,1000,US_09_02_MOUNTAIN_TOURISM_CONTEST_DESC,CONTESTS\nUS_10_01_CONSTRUCTION_DRILLING_STATION_01_OBJ,US_10_01_CONSTRUCTION_DRILLING_STATION_01_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,2\u00d7 Metal Planks,1250,11150,US_10_01_CONSTRUCTION_DRILLING_STATION_01_DESC,CONTRACTS\nUS_10_01_DELIVERY_TRAILER_01_OBJ,US_10_01_DELIVERY_TRAILER_01_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,,660,6250,US_10_01_DELIVERY_TRAILER_01_OBJ_DESC,CONTRACTS\nUS_10_01_ELECTRIC_POLE_01_OBJ,US_10_01_ELECTRIC_POLE_01_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts,580,6300,US_10_01_ELECTRIC_POLE_01_DESC,CONTRACTS\nUS_10_01_DELIVERY_LOG_01_OBJ,US_10_01_DELIVERY_LOG_01_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Logs Medium,440,3400,US_10_01_DELIVERY_LOG_01_DESC,CONTRACTS\nUS_10_01_DELIVERY_OILTANK_01_TSK,US_10_01_DELIVERY_OILTANK_01_TSK,_TASKS,US_10,British Columbia,truckDelivery,,340,2900,US_10_01_DELIVERY_OILTANK_01_DESC,TASKS\nUS_10_01_FALLEN_POWER_LINE_01_TSK,US_10_01_FALLEN_POWER_LINE_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts,730,8050,US_10_01_FALLEN_POWER_LINE_01_DESC,TASKS\nUS_10_01_PIPELINE_01_TSK,US_10_01_PIPELINE_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Pipe Large,790,7400,US_10_01_PIPELINE_01_DESC,TASKS\nUS_10_01_PASS_01_TSK,US_10_01_PASS_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Concrete Slab; 1\u00d7 Service Spare Parts,650,6250,US_10_01_PASS_01_DESC,TASKS\nUS_10_01_FIX_BRIDGE_01_TSK,US_10_01_FIX_BRIDGE_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,2\u00d7 Metal Planks,750,5850,US_10_01_FIX_BRIDGE_01_DESC,TASKS\nUS_10_01_CONSTRUCTION_DRILLING_STATION_02_OBJ,US_10_01_CONSTRUCTION_DRILLING_STATION_02_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Pipes Small; 2\u00d7 Service Spare Parts; 1\u00d7 Metal Planks,1530,15200,US_10_01_CONSTRUCTION_DRILLING_STATION_02_DESC,CONTRACTS\nUS_10_01_ELECTRIC_POLE_02_OBJ,US_10_01_ELECTRIC_POLE_02_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts,600,7500,US_10_01_ELECTRIC_POLE_02_DESC,CONTRACTS\nUS_10_01_CONSTRUCTION_WAREHOUSE_02_OBJ,US_10_01_CONSTRUCTION_WAREHOUSE_02_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Crate Large; 1\u00d7 Wooden Planks,430,4750,US_10_01_CONSTRUCTION_WAREHOUSE_02_DESC,CONTRACTS\nUS_10_01_DELIVERY_LOG_02_OBJ,US_10_01_DELIVERY_LOG_02_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,4\u00d7 Wooden Planks,210,1750,US_10_01_DELIVERY_LOG_02_DESC,CONTRACTS\nUS_10_01_DELIVERY_OILTANK_02_TSK,US_10_01_DELIVERY_OILTANK_02_TSK,_TASKS,US_10,British Columbia,truckDelivery,,370,3100,US_10_01_DELIVERY_OILTANK_02_DESC,TASKS\nUS_10_01_FALLEN_POWER_LINE_02_TSK,US_10_01_FALLEN_POWER_LINE_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts,850,10050,US_10_01_FALLEN_POWER_LINE_02_DESC,TASKS\nUS_10_01_PIPELINE_02_TSK,US_10_01_PIPELINE_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Pipe Large,730,6450,US_10_01_PIPELINE_02_DESC,TASKS\nUS_10_01_FIX_BRIDGE_02_TSK,US_10_01_FIX_BRIDGE_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,3\u00d7 Wooden Planks,240,2600,US_10_01_FIX_BRIDGE_02_DESC,TASKS\nUS_10_01_PASS_02_TSK,US_10_01_PASS_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Concrete Slab; 1\u00d7 Service Spare Parts,920,1050,US_10_01_PASS_02_DESC,TASKS\nUS_10_01_CONSTRUCTION_DRILLING_STATION_03_OBJ,US_10_01_CONSTRUCTION_DRILLING_STATION_03_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Container Large; 1\u00d7 Service Spare Parts,970,8300,US_10_01_CONSTRUCTION_DRILLING_STATION_03_DESC,CONTRACTS\nUS_10_01_FIX_BRIDGE_03_TSK,US_10_01_FIX_BRIDGE_03_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Logs Medium,560,7750,US_10_01_FIX_BRIDGE_03_DESC,TASKS\nUS_10_01_CONSTRUCTION_DRILLING_STATION_04_OBJ,US_10_01_CONSTRUCTION_DRILLING_STATION_04_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,2\u00d7 Pipes Small; 1\u00d7 Container Large Drilling,1690,13550,US_10_01_CONSTRUCTION_DRILLING_STATION_04_DESC,CONTRACTS\nUS_10_01_FIX_BRIDGE_04_TSK,US_10_01_FIX_BRIDGE_04_TSK,_TASKS,US_10,British Columbia,cargoDelivery,2\u00d7 Logs Short,280,3700,US_10_01_FIX_BRIDGE_04_DESC,TASKS\nUS_10_01_FIX_BRIDGE_05_TSK,US_10_01_FIX_BRIDGE_05_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Logs Short,300,2450,US_10_01_FIX_BRIDGE_05_DESC,TASKS\nUS_10_01_CONSTRUCTION_TWO_WAREHOUSE_OBJ,US_10_01_CONSTRUCTION_TWO_WAREHOUSE_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts; 2\u00d7 Container Small; 2\u00d7 Pipes Small,2060,17200,US_10_01_CONSTRUCTION_TWO_WAREHOUSE_DESC,CONTRACTS\nUS_10_01_CONSTRUCTION_LOCAL_BUSINESS_OBJ,US_10_01_CONSTRUCTION_LOCAL_BUSINESS_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Container Large; 1\u00d7 Service Spare Parts,1700,17050,US_10_01_CONSTRUCTION_LOCAL_BUSINESS_DESC,CONTRACTS\nUS_10_01_CONSTRUCTION_RAILWAY_OBJ,US_10_01_CONSTRUCTION_RAILWAY_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Concrete Slab; 1\u00d7 Wooden Planks; 1\u00d7 Container Small,1240,13000,US_10_01_CONSTRUCTION_RAILWAY_DESC,CONTRACTS\nUS_10_01_CONSTRUCTION_FACTORY_OBJ,US_10_01_CONSTRUCTION_FACTORY_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts; 1\u00d7 Container Small,1080,10800,US_10_01_CONSTRUCTION_FACTORY_DESC,CONTRACTS\nUS_10_01_EXIT_CLEANING_OBJ,US_10_01_EXIT_CLEANING_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts; 2\u00d7 Concrete Slab,1140,10200,US_10_01_EXIT_CLEANING_DESC,CONTRACTS\nUS_10_01_DELIVERY_FERRY_OBJ,US_10_01_DELIVERY_FERRY_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Crate Large,650,6550,US_10_01_DELIVERY_FERRY_DESC,CONTRACTS\nUS_10_01_DELIVERY_TRANSFORMER_TSK,US_10_01_DELIVERY_TRANSFORMER_TSK,_TASKS,US_10,British Columbia,truckDelivery,,350,3350,US_10_01_DELIVERY_TRANSFORMER_DESC,TASKS\nUS_10_01_DELIVERY_TRUCK_TSK,US_10_01_DELIVERY_TRUCK_TSK,_TASKS,US_10,British Columbia,truckDelivery,,340,2900,US_10_01_DELIVERY_TRUCK_DESC,TASKS\nUS_10_01_DELIVERY_OIL_RIGS_TSK,US_10_01_DELIVERY_OIL_RIGS_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Big Drills; 1\u00d7 Big Drills; 1\u00d7 Big Drills,1200,13900,US_10_01_DELIVERY_OIL_RIGS_DESC,TASKS\nUS_10_01_FALLEN_TRAIN_TSK,US_10_01_FALLEN_TRAIN_TSK,_TASKS,US_10,British Columbia,cargoDelivery,4\u00d7 Gold,1540,13000,US_10_01_FALLEN_TRAIN_DESC,TASKS\nUS_10_01_CONSTRUCTION_GAS_WAREHOUSE_TSK,US_10_01_CONSTRUCTION_GAS_WAREHOUSE_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Container Small,1110,11250,US_10_01_CONSTRUCTION_GAS_WAREHOUSE_DESC,TASKS\nUS_10_01_DELIVERY_METEO_TSK,US_10_01_DELIVERY_METEO_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Container Small,500,8600,US_10_01_DELIVERY_METEO_DESC,TASKS\nUS_10_01_DELIVERY_HUNT_TSK,US_10_01_DELIVERY_HUNT_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Crate Large; 1\u00d7 Logs Short,600,6200,US_10_01_DELIVERY_HUNT_DESC,TASKS\nUS_10_01_DELIVERY_LIFT_TSK,US_10_01_DELIVERY_LIFT_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts,400,3800,US_10_01_DELIVERY_LIFT_DESC,TASKS\nUS_10_01_CONT_VISIT,US_10_01_CONT_VISIT,_CONTESTS,US_10,British Columbia,exploration,,150,2000,US_10_01_CONT_VISIT_DESC,CONTESTS\nUS_10_01_CONTEST_RACE,US_10_01_CONTEST_RACE,_CONTESTS,US_10,British Columbia,exploration,,125,2000,US_10_01_CONTEST_RACE_DESC,CONTESTS\nUS_10_02_DELIVERY_BOAT_01_OBJ,US_10_02_DELIVERY_BOAT_01_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,,760,9500,US_10_02_DELIVERY_BOAT_01_DESC,CONTRACTS\nUS_10_02_WILDLIFE_OFFICERS_01_OILTANK_OBJ,US_10_02_WILDLIFE_OFFICERS_01_OILTANK_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,,500,5300,US_10_02_DELIVERY_OILTANK_02_DESC,CONTRACTS\nUS_10_02_DELIVERY_TRUCK_01_OBJ,US_10_02_DELIVERY_TRUCK_01_OBJ,_CONTRACTS,US_10,British Columbia,truckDelivery,,500,5250,US_10_02_DELIVERY_TRUCK_01_DESC,CONTRACTS\nUS_10_02_DELIVERY_CARGO_01_OBJ,US_10_02_DELIVERY_CARGO_01_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Container Small Special,900,9650,US_10_02_DELIVERY_CARGO_01_DESC,CONTRACTS\nUS_10_02_DELIVERY_TRAILER_01_TSK,US_10_02_DELIVERY_TRAILER_01_TSK,_TASKS,US_10,British Columbia,truckDelivery,,840,8700,US_10_02_DELIVERY_TRAILER_01_DESC,TASKS\nUS_10_02_DELIVERY_TRUCK_01_TSK,US_10_02_DELIVERY_TRUCK_01_TSK,_TASKS,US_10,British Columbia,truckDelivery,,420,4000,US_10_02_DELIVERY_TRUCK_01_TSK_DESC,TASKS\nUS_10_02_DELIVERY_OILTANK_01_TSK,US_10_02_DELIVERY_OILTANK_01_TSK,_TASKS,US_10,British Columbia,truckDelivery,,360,3150,US_10_02_DELIVERY_OILTANK_01_DESC,TASKS\nUS_10_02_FIX_BRIDGE_01_TSK,US_10_02_FIX_BRIDGE_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Logs Medium,620,7750,US_10_02_FIX_BRIDGE_01_DESC,TASKS\nUS_10_02_WAREHOUSE_01_CONSTRUCTION_TSK,US_10_02_WAREHOUSE_01_CONSTRUCTION_TSK,_TASKS,US_10,British Columbia,cargoDelivery,2\u00d7 Metal Roll; 2\u00d7 Bags,780,7050,US_10_02_WAREHOUSE_01_CONSTRUCTION_DESC,TASKS\nUS_10_02_REMOVE_STONES_01_TSK,US_10_02_REMOVE_STONES_01_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts,270,2800,US_10_02_REMOVE_STONES_01_DESC,TASKS\nUS_10_02_SCOUT_VISIT_01_TSK,US_10_02_SCOUT_VISIT_01_TSK,_TASKS,US_10,British Columbia,exploration,,290,4950,US_10_02_SCOUT_VISIT_01_DESC,TASKS\nUS_10_02_CONT_01,US_10_02_CONT_01,_CONTESTS,US_10,British Columbia,exploration,,170,3200,US_10_02_CONT_01_DESC,CONTESTS\nUS_10_02_WILDLIFE_OFFICERS_02_VISIT_OBJ,US_10_02_WILDLIFE_OFFICERS_02_VISIT_OBJ,_CONTRACTS,US_10,British Columbia,exploration,,140,2100,US_10_02_SCOUT_VISIT_02_DESC,CONTRACTS\nUS_10_02_DELIVERY_TRAILER_02_SCOUT_TSK,US_10_02_DELIVERY_TRAILER_02_SCOUT_TSK,_TASKS,US_10,British Columbia,truckDelivery,,500,5000,US_10_02_DELIVERY_TRAILER_02_DESC,TASKS\nUS_10_02_DELIVERY_TRUCK_02_TSK,US_10_02_DELIVERY_TRUCK_02_TSK,_TASKS,US_10,British Columbia,truckDelivery,,440,4250,US_10_02_DELIVERY_TRUCK_02_DESC,TASKS\nUS_10_02_FIX_BRIDGE_02_TSK,US_10_02_FIX_BRIDGE_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Wooden Planks,800,9700,US_10_02_FIX_BRIDGE_02_DESC,TASKS\nUS_10_02_REMOVE_STONES_02_TSK,US_10_02_REMOVE_STONES_02_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts,160,1350,US_10_02_REMOVE_STONES_02_DESC,TASKS\nUS_10_02_CONT_02,US_10_02_CONT_02,_CONTESTS,US_10,British Columbia,cargoDelivery,2\u00d7 Barrels Chemicals,600,5000,US_10_02_CONT_02_DESC,CONTESTS\nUS_10_02_WILDLIFE_OFFICERS_03_BOAT_OBJ,US_10_02_WILDLIFE_OFFICERS_03_BOAT_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Service Spare Parts Special,770,9650,US_10_02_DELIVERY_CARGO_04_DESC,CONTRACTS\nUS_10_02_DELIVERY_TRUCK_03_TSK,US_10_02_DELIVERY_TRUCK_03_TSK,_TASKS,US_10,British Columbia,truckDelivery,,490,5150,US_10_02_DELIVERY_TRUCK_03_DESC,TASKS\nUS_10_02_CANNERY_CONSTRUCTION_OBJ,US_10_02_CANNERY_CONSTRUCTION_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Bricks; 2\u00d7 Metal Planks; 2\u00d7 Crate Large; 2\u00d7 Concrete Slab,2110,18950,US_10_02_CANNERY_CONSTRUCTION_DESC,CONTRACTS\nUS_10_02_GARAGE_CONSTRUCTION_OBJ,US_10_02_GARAGE_CONSTRUCTION_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Bricks; 2\u00d7 Vehicles Spare Parts,1380,16600,US_10_02_GARAGE_CONSTRUCTION_DESC,CONTRACTS\nUS_10_02_TELEPHONE_TOWER_OBJ,US_10_02_TELEPHONE_TOWER_OBJ,_CONTRACTS,US_10,British Columbia,cargoDelivery,1\u00d7 Container Small; 1\u00d7 Service Spare Parts,820,8700,US_10_02_TELEPHONE_TOWER_DESC,CONTRACTS\nUS_10_02_FARM_TOWER_CONSTRUCTION_TSK,US_10_02_FARM_TOWER_CONSTRUCTION_TSK,_TASKS,US_10,British Columbia,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Container Small,940,9550,US_10_02_FARM_TOWER_CONSTRUCTION_DESC,TASKS\nUS_10_02_REMOVE_TOWERS_TSK,US_10_02_REMOVE_TOWERS_TSK,_TASKS,US_10,British Columbia,cargoDelivery,2\u00d7 Service Spare Parts,450,5150,US_10_02_REMOVE_TOWER_01_DESC,TASKS\nUS_11_01_BLOCKAGE_ON_THE_ROAD_01_TASK,US_11_01_BLOCKAGE_ON_THE_ROAD_01_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,4\u00d7 Wooden Planks,440,5000,US_11_01_BLOCKAGE_ON_THE_ROAD_01_TASK_DESC,TASKS\nUS_11_01_BLOCKAGE_ON_THE_ROAD_02_TASK,US_11_01_BLOCKAGE_ON_THE_ROAD_02_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,2\u00d7 Wooden Planks,310,3500,US_11_01_BLOCKAGE_ON_THE_ROAD_02_TASK_DESC,TASKS\nUS_11_01_EQUIPMENT_SET_MOUNTAIN_CONTRACT,US_11_01_EQUIPMENT_SET_MOUNTAIN_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,1\u00d7 Plane,1280,13400,US_11_01_EQUIPMENT_SET_MOUNTAIN_CONTRACT_DESC,CONTRACTS\nUS_11_01_CABLE_CAR_CONTRACT,US_11_01_CABLE_CAR_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,2\u00d7 Bags; 2\u00d7 Metal Roll,1340,12600,US_11_01_CABLE_CAR_CONTRACT_DESC,CONTRACTS\nUS_11_01_REPAIR_WAREHOUSE_CONTRACT,US_11_01_REPAIR_WAREHOUSE_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,2\u00d7 Metal Roll; 1\u00d7 Metal Planks,1270,11450,US_11_01_REPAIR_WAREHOUSE_CONTRACT_DESC,CONTRACTS\nUS_11_01_FILM_STAR_MOUNTAIN_CONTRACT,US_11_01_FILM_STAR_MOUNTAIN_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,700,8400,US_11_01_FILM_STAR_MOUNTAIN_CONTRACT_DESC,CONTRACTS\nUS_11_01_CITY_CONSTRUCTION_CONTRACT,US_11_01_CITY_CONSTRUCTION_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,540,6000,US_11_01_CITY_CONSTRUCTION_CONTRACT_DESC,CONTRACTS\nUS_11_01_REQUIRED_TOOLS_CONTRACT,US_11_01_REQUIRED_TOOLS_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,510,5500,US_11_01_REQUIRED_TOOLS_CONTRACT_DESC,CONTRACTS\nUS_11_01_LOST_TRAILERS_CONTRACT,US_11_01_LOST_TRAILERS_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,430,4800,US_11_01_LOST_TRAILERS_CONTRACT_DESC,CONTRACTS\nUS_11_01_FILM_STAR_CITY_CONTRACT,US_11_01_FILM_STAR_CITY_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,440,4600,US_11_01_FILM_STAR_CITY_CONTRACT_DESC,CONTRACTS\nUS_11_01_STAR_TRAILER_MOUNTAIN_CONTRACT,US_11_01_STAR_TRAILER_MOUNTAIN_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,,420,3850,US_11_01_STAR_TRAILER_MOUNTAIN_DESC,CONTRACTS\nUS_11_01_CABIN_IN_THE_MOUNTAIN_CONTRACT,US_11_01_CABIN_IN_THE_MOUNTAIN_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,3\u00d7 Wooden Planks; 1\u00d7 Logs Medium; 1\u00d7 Bags; 2\u00d7 Wooden Planks Medium,2000,21200,US_11_01_CABIN_IN_THE_MOUNTAIN_CONTRACT_DESC,CONTRACTS\nUS_11_01_HOLLYWOOD_SMILE_CONTRACT,US_11_01_HOLLYWOOD_SMILE_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,4\u00d7 Metal Roll; 4\u00d7 Concrete Slab,2450,21000,US_11_01_HOLLYWOOD_SMILE_CONTRACT_DESC,CONTRACTS\nUS_11_01_HOLIDAY_BEGINS_WITH_WORK_CONTRACT,US_11_01_HOLIDAY_BEGINS_WITH_WORK_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,4\u00d7 Metal Planks; 2\u00d7 Metal Roll; 2\u00d7 Wooden Planks; 2\u00d7 Forklift Caravan Container 2,2470,20850,US_11_01_HOLIDAY_BEGINS_WITH_WORK_CONTRACT_DESC,CONTRACTS\nUS_11_01_CABIN_IN_THE_WOODS_CONTRACT,US_11_01_CABIN_IN_THE_WOODS_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Crate Large; 1\u00d7 Logs Short; 3\u00d7 Wooden Planks,1670,19350,US_11_01_CABIN_IN_THE_WOODS_CONTRACT_DESC,CONTRACTS\nUS_11_01_VIEW_FROM_ABOVE_CONTRACT,US_11_01_VIEW_FROM_ABOVE_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,4\u00d7 Cable Car,1680,18800,US_11_01_VIEW_FROM_ABOVE_CONTRACT_DESC,CONTRACTS\nUS_11_01_TOP_VIEW_CONTRACT,US_11_01_TOP_VIEW_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Logs Short; 3\u00d7 Crate Large,1190,12750,US_11_01_TOP_VIEW_CONTRACT_DESC,CONTRACTS\nUS_11_01_FORESTER_IS_WATCHING_YOU_CONTRACT,US_11_01_FORESTER_IS_WATCHING_YOU_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Metal Roll,1200,11700,US_11_01_FORESTER_IS_WATCHING_YOU_CONTRACT_DESC,CONTRACTS\nUS_11_01_CLEAR_MOUNTAIN_CONTRACT,US_11_01_CLEAR_MOUNTAIN_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Rubbish,930,10150,US_11_01_CLEAR_MOUNTAIN_CONTRACT_DESC,CONTRACTS\nUS_11_01_HELP_FOR_DROWNING_CONTRACT,US_11_01_HELP_FOR_DROWNING_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Crate Large; 1\u00d7 Crate Large; 1\u00d7 Crate Large; 2\u00d7 Crate Large,980,9150,US_11_01_HELP_FOR_DROWNING_CONTRACT_DESC,CONTRACTS\nUS_11_01_EQUIPMENT_SET_CITY_CONTRACT,US_11_01_EQUIPMENT_SET_CITY_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Metal Planks; 6\u00d7 Wooden Planks,890,7950,US_11_01_EQUIPMENT_SET_CITY_CONTRACT_DESC,CONTRACTS\nUS_11_01_FALLING_TOOLS_CONTRACT,US_11_01_FALLING_TOOLS_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,3\u00d7 Service Spare Parts,520,4800,US_11_01_FALLING_TOOLS_CONTRACT_DESC,CONTRACTS\nUS_11_01_TERRIORY_EXPLORATION_CONTRACT,US_11_01_TERRIORY_EXPLORATION_CONTRACT,_CONTRACTS,US_11,Scandinavia,exploration,,120,2100,US_11_01_TERRIORY_EXPLORATION_CONTRACT_DESC,CONTRACTS\nUS_11_01_LOST_CARGO_TASK,US_11_01_LOST_CARGO_TASK,_TASKS,US_11,Scandinavia,truckDelivery,2\u00d7 Service Spare Parts,750,8100,US_11_01_LOST_CARGO_TASK_DESC,TASKS\nUS_11_01_CORNERED_TASK,US_11_01_CORNERED_TASK,_TASKS,US_11,Scandinavia,truckDelivery,2\u00d7 Service Spare Parts,650,5350,US_11_01_CORNERED_TASK_DESC,TASKS\nUS_11_01_STRUCK_NOT_BROKEN_TASK,US_11_01_STRUCK_NOT_BROKEN_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,490,5150,US_11_01_STRUCK_NOT_BROKEN_TASK_DESC,TASKS\nUS_11_01_RECHARGING_TASK,US_11_01_RECHARGING_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,440,4700,US_11_01_RECHARGING_TASK_DESC,TASKS\nUS_11_01_TAMER_TROLLS_TASK,US_11_01_TAMER_TROLLS_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,450,4400,US_11_01_TAMER_TROLLS_TASK_DESC,TASKS\nUS_11_01_FRIENDLY_HELP_TASK,US_11_01_FRIENDLY_HELP_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,420,4200,US_11_01_FRIENDLY_HELP_TASK_DESC,TASKS\nUS_11_01_MISTAKES_HAPPEN_TASK,US_11_01_MISTAKES_HAPPEN_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,430,3800,US_11_01_MISTAKES_HAPPEN_TASK_DESC,TASKS\nUS_11_01_LEFT_TASK,US_11_01_LEFT_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,400,3600,US_11_01_LEFT_TASK_DESC,TASKS\nUS_11_01_FAILED_MOVE_TASK,US_11_01_FAILED_MOVE_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,420,3500,US_11_01_FAILED_MOVE_TASK_DESC,TASKS\nUS_11_01_WRONG_TURN_TASK,US_11_01_WRONG_TURN_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,360,3200,US_11_01_WRONG_TURN_TASK_DESC,TASKS\nUS_11_01_FALLEN_CAR_TASK,US_11_01_FALLEN_CAR_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,360,2850,US_11_01_FALLEN_CAR_TASK_DESC,TASKS\nUS_11_01_BRIDGE_REPAIR_TASK,US_11_01_BRIDGE_REPAIR_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,2\u00d7 Metal Planks; 1\u00d7 Blocks,1070,8850,US_11_01_BRIDGE_REPAIR_TASK_DESC,TASKS\nUS_11_01_DROWNED_ORDER_TASK,US_11_01_DROWNED_ORDER_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,3\u00d7 Cellulose,680,5900,US_11_01_DROWNED_ORDER_TASK_DESC,TASKS\nUS_11_01_FAIL_TASK,US_11_01_FAIL_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,2\u00d7 Barrels,500,4950,US_11_01_FAIL_TASK_DESC,TASKS\nUS_11_01_FLOODED_RACE_CONTEST,US_11_01_FLOODED_RACE_CONTEST,_CONTESTS,US_11,Scandinavia,exploration,,150,750,US_11_01_FLOODED_RACE_CONTEST_DESC,CONTESTS\nUS_11_02_MOVIE_PLACE_CONTRACT_01_02,US_11_02_MOVIE_PLACE_CONTRACT_01_02,_CONTRACTS,US_11,Scandinavia,truckDelivery,,420,4750,US_11_02_MOVIE_PLACE_CONTRACT_01_02_DESC,CONTRACTS\nUS_11_02_MOVIE_PLACE_CONTRACT_01,US_11_02_MOVIE_PLACE_CONTRACT_01,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Container Small; 2\u00d7 Wooden Planks,770,7500,US_11_02_MOVIE_PLACE_CONTRACT_01_DESC,CONTRACTS\nUS_11_02_RECYCLING_RECOVERY_CONTRACT_01,US_11_02_RECYCLING_RECOVERY_CONTRACT_01,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Bags; 2\u00d7 Service Spare Parts,560,5050,US_11_02_RECYCLING_RECOVERY_CONTRACT_01_DESC,CONTRACTS\nUS_11_02_WATCHTOWER_CONTRACT_01,US_11_02_WATCHTOWER_CONTRACT_01,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Logs Medium; 2\u00d7 Metal Roll,490,4600,US_11_02_WATCHTOWER_CONTRACT_01_DESC,CONTRACTS\nUS_11_02_TRACK_DELIVERY_TASK_01,US_11_02_TRACK_DELIVERY_TASK_01,_TASKS,US_11,Scandinavia,truckDelivery,,360,3550,US_11_02_TRACK_DELIVERY_TASK_01_DESC,TASKS\nUS_11_02_PATH_TASK_01,US_11_02_PATH_TASK_01,_TASKS,US_11,Scandinavia,cargoDelivery,1\u00d7 Metal Planks,220,1750,US_11_02_PATH_TASK_01_DESC,TASKS\nUS_11_02_MOVIE_PLACE_CONTRACT_02_02,US_11_02_MOVIE_PLACE_CONTRACT_02_02,_CONTRACTS,US_11,Scandinavia,truckDelivery,,350,3450,US_11_02_MOVIE_PLACE_CONTRACT_02_02_DESC,CONTRACTS\nUS_11_02_RECYCLING_RECOVERY_CONTRACT_02,US_11_02_RECYCLING_RECOVERY_CONTRACT_02,_CONTRACTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Rubbish; 1\u00d7 Rubbish,630,6500,US_11_02_RECYCLING_RECOVERY_CONTRACT_02_DESC,CONTRACTS\nUS_11_02_WATCHTOWER_CONTRACT_02,US_11_02_WATCHTOWER_CONTRACT_02,_CONTRACTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Logs Long; 2\u00d7 Metal Roll,700,5550,US_11_02_WATCHTOWER_CONTRACT_02_DESC,CONTRACTS\nUS_11_02_MOVIE_PLACE_CONTRACT_02,US_11_02_MOVIE_PLACE_CONTRACT_02,_CONTRACTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Service Spare Parts; 2\u00d7 Metal Planks,560,4100,US_11_02_MOVIE_PLACE_CONTRACT_02_DESC,CONTRACTS\nUS_11_02_TRACK_DELIVERY_TASK_02,US_11_02_TRACK_DELIVERY_TASK_02,_TASKS,US_11,Scandinavia,truckDelivery,1\u00d7 Bricks; 1\u00d7 Cellulose; 2\u00d7 Bags,310,2750,US_11_02_TRACK_DELIVERY_TASK_02_DESC,TASKS\nUS_11_02_PATH_TASK_02,US_11_02_PATH_TASK_02,_TASKS,US_11,Scandinavia,cargoDelivery,1\u00d7 Metal Planks,360,3050,US_11_02_PATH_TASK_02_DESC,TASKS\nUS_11_02_HOTEL_HOUSE_CONTRACT,US_11_02_HOTEL_HOUSE_CONTRACT,_CONTRACTS,US_11,Scandinavia,truckDelivery,2\u00d7 Bags; 4\u00d7 Wooden Planks,690,5500,US_11_02_HOTEL_HOUSE_CONTRACT_DESC,CONTRACTS\nUS_11_02_FIXING_STATION_CONTRACT,US_11_02_FIXING_STATION_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,3\u00d7 Rubbish; 2\u00d7 Metal Planks,1480,13700,US_11_02_FIXING_STATION_CONTRACT_DESC,CONTRACTS\nUS_11_02_RAIL_REPAIR_CONTRACT,US_11_02_RAIL_REPAIR_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Railway; 1\u00d7 Railway,840,7300,US_11_02_RAIL_REPAIR_CONTRACT_DESC,CONTRACTS\nUS_11_02_WATER_CAMP_CONTRACT,US_11_02_WATER_CAMP_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,450\u00d7 Water; 250\u00d7 Water,1000,6000,US_11_02_WATER_CAMP_CONTRACT_DESC,CONTRACTS\nUS_11_02_CAMP_CONTRACT,US_11_02_CAMP_CONTRACT,_CONTRACTS,US_11,Scandinavia,cargoDelivery,2\u00d7 Logs Short,410,4450,US_11_02_CAMP_CONTRACT_DESC,CONTRACTS\nUS_11_02_RESEARCH_CONTRACT,US_11_02_RESEARCH_CONTRACT,_CONTRACTS,US_11,Scandinavia,exploration,,150,2500,US_11_02_RESEARCH_CONTRACT_DESC,CONTRACTS\nUS_11_02_SEARCH_TASK,US_11_02_SEARCH_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,650,8000,US_11_02_SEARCH_TASK_DESC,TASKS\nUS_11_02_ACCIDENT_TASK,US_11_02_ACCIDENT_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,340,3300,US_11_02_ACCIDENT_TASK_DESC,TASKS\nUS_11_02_NEO_FALCON_TASK,US_11_02_NEO_FALCON_TASK,_TASKS,US_11,Scandinavia,truckDelivery,,210,1800,US_11_02_NEO_FALCON_TASK_DESC,TASKS\nUS_11_02_LOST_CARGO_TASK,US_11_02_LOST_CARGO_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,4\u00d7 Service Spare Parts,480,4350,US_11_02_LOST_CARGO_TASK_DESC,TASKS\nUS_11_02_WOODEN_BRIDGE_TASK,US_11_02_WOODEN_BRIDGE_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,1\u00d7 Logs Long; 2\u00d7 Wooden Planks,620,3850,US_11_02_WOODEN_BRIDGE_TASK_DESC,TASKS\nUS_11_02_BRIDGE_TASK,US_11_02_BRIDGE_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,2\u00d7 Bags; 2\u00d7 Metal Planks,500,3850,US_11_02_BRIDGE_TASK_DESC,TASKS\nUS_11_02_REPAIR_TASK,US_11_02_REPAIR_TASK,_TASKS,US_11,Scandinavia,cargoDelivery,4\u00d7 Vehicles Spare Parts,330,3150,US_11_02_REPAIR_TASK_DESC,TASKS\nUS_11_02_VISIT_TASK,US_11_02_VISIT_TASK,_TASKS,US_11,Scandinavia,exploration,,250,4200,US_11_02_VISIT_TASK_DESC,TASKS\nUS_11_02_SEISMO_TASK,US_11_02_SEISMO_TASK,_TASKS,US_11,Scandinavia,exploration,,250,1500,US_11_02_SEISMO_TASK_DESC,TASKS\nUS_11_02_DELIVERY,US_11_02_DELIVERY,_CONTESTS,US_11,Scandinavia,cargoDelivery,1\u00d7 Gold,150,750,US_11_02_DELIVERY_DESC,CONTESTS\nUS_11_02_DESCENT_CONTEST,US_11_02_DESCENT_CONTEST,_CONTESTS,US_11,Scandinavia,exploration,,75,250,US_11_02_DESCENT_CONTEST_DESC,CONTESTS\nUS_12_01_POWER_LINES_CONTRACT_01,US_12_01_POWER_LINES_CONTRACT_01,_CONTRACTS,US_12,North Carolina,truckDelivery,,600,7250,US_12_01_POWER_LINES_CONTRACT_01_DESC,CONTRACTS\nUS_12_01_WEATHER_STATION_CONTRACT_01,US_12_01_WEATHER_STATION_CONTRACT_01,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Solar Panel; 1\u00d7 Solar Panel; 1\u00d7 Metal Planks; 1\u00d7 Solar Panel; 1\u00d7 Metal Planks,1460,16000,US_12_01_WEATHER_STATION_CONTRACT_01_DESC,CONTRACTS\nUS_12_01_RESTORATION_CROSSINGS_CONTRACT_01,US_12_01_RESTORATION_CROSSINGS_CONTRACT_01,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Concrete Slab,650,5050,US_12_01_RESTORATION_CROSSINGS_CONTRACT_01_DESC,CONTRACTS\nUS_12_01_WOODEN_BRIDGE_TASK_01,US_12_01_WOODEN_BRIDGE_TASK_01,_TASKS,US_12,North Carolina,cargoDelivery,3\u00d7 Wooden Planks Medium,400,5050,US_12_01_WOODEN_BRIDGE_TASK_01_DESC,TASKS\nUS_12_01_VALUE_TASK_01,US_12_01_VALUE_TASK_01,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Metal Planks,290,2350,US_12_01_VALUE_TASK_01_DESC,TASKS\nUS_12_01_WEATHER_STATION_CONTRACT_02,US_12_01_WEATHER_STATION_CONTRACT_02,_CONTRACTS,US_12,North Carolina,cargoDelivery,3\u00d7 Service Spare Parts; 3\u00d7 Service Spare Parts,800,8550,US_12_01_WEATHER_STATION_CONTRACT_02_DESC,CONTRACTS\nUS_12_01_RESTORATION_CROSSINGS_CONTRACT_02,US_12_01_RESTORATION_CROSSINGS_CONTRACT_02,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Container Small,770,7000,US_12_01_RESTORATION_CROSSINGS_CONTRACT_02_DESC,CONTRACTS\nUS_12_01_POWER_LINES_CONTRACT_02,US_12_01_POWER_LINES_CONTRACT_02,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Metal Planks,560,5200,US_12_01_POWER_LINES_CONTRACT_02_DESC,CONTRACTS\nUS_12_01_WOODEN_BRIDGE_TASK_02,US_12_01_WOODEN_BRIDGE_TASK_02,_TASKS,US_12,North Carolina,cargoDelivery,3\u00d7 Wooden Planks Medium,410,5200,US_12_01_WOODEN_BRIDGE_TASK_02_DESC,TASKS\nUS_12_01_VALUE_TASK_02,US_12_01_VALUE_TASK_02,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Metal Planks,290,2350,US_12_01_VALUE_TASK_02_DESC,TASKS\nUS_12_01_POWER_LINES_CONTRACT_03,US_12_01_POWER_LINES_CONTRACT_03,_CONTRACTS,US_12,North Carolina,truckDelivery,1\u00d7 Metal Planks,800,8000,US_12_01_POWER_LINES_CONTRACT_03_DESC,CONTRACTS\nUS_12_01_TRANSFORMER_CONTRACT,US_12_01_TRANSFORMER_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,3\u00d7 Metal Planks; 4\u00d7 Barrels; 3\u00d7 Metal Planks,2960,27800,US_12_01_TRANSFORMER_CONTRACT_DESC,CONTRACTS\nUS_12_01_BIG_BRIDGE_CONTRACT,US_12_01_BIG_BRIDGE_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,3\u00d7 Metal Planks; 4\u00d7 Bags,1650,13550,US_12_01_BIG_BRIDGE_CONTRACT_DESC,CONTRACTS\nUS_12_01_SPECIAL_BRIDGE_CONTRACT,US_12_01_SPECIAL_BRIDGE_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,4\u00d7 Barrels,1120,12350,US_12_01_SPECIAL_BRIDGE_CONTRACT_DESC,CONTRACTS\nUS_12_01_SCIENTIFIC_MACHINE_CONTRACT,US_12_01_SCIENTIFIC_MACHINE_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,1\u00d7 Metal Planks,950,10000,US_12_01_SCIENTIFIC_MACHINE_CONTRACT_DESC,CONTRACTS\nUS_12_01_LOG_FOR_STORAGE_CONTRACT,US_12_01_LOG_FOR_STORAGE_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Logs Medium,1400,16500,US_12_01_LOG_FOR_STORAGE_CONTRACT_DESC,CONTRACTS\nUS_12_01_TELESCOPE_CONTRACT,US_12_01_TELESCOPE_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,3\u00d7 Metal Roll; 4\u00d7 Crate Large,1070,9550,US_12_01_TELESCOPE_CONTRACT_DESC,CONTRACTS\nUS_12_01_WATER_PROBE_CONTRACT,US_12_01_WATER_PROBE_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy,570,5800,US_12_01_WATER_PROBE_CONTRACT_DESC,CONTRACTS\nUS_12_01_SCIENTIFIC_INTELLIGENCE_CONTRACT,US_12_01_SCIENTIFIC_INTELLIGENCE_CONTRACT,_CONTRACTS,US_12,North Carolina,exploration,,350,6000,US_12_01_SCIENTIFIC_INTELLIGENCE_CONTRACT_DESC,CONTRACTS\nUS_12_01_SCIENTIFIC_CONTINUED_TASK,US_12_01_SCIENTIFIC_CONTINUED_TASK,_TASKS,US_12,North Carolina,truckDelivery,,510,5950,US_12_01_SCIENTIFIC_CONTINUED_TASK_DESC,TASKS\nUS_12_01_LEFT_LOAD_TASK,US_12_01_LEFT_LOAD_TASK,_TASKS,US_12,North Carolina,truckDelivery,4\u00d7 Bags,720,5750,US_12_01_LEFT_LOAD_TASK_DESC,TASKS\nUS_12_01_CAR_SWAMP_TASK,US_12_01_CAR_SWAMP_TASK,_TASKS,US_12,North Carolina,truckDelivery,,350,3800,US_12_01_CAR_SWAMP_TASK_DESC,TASKS\nUS_12_01_CAR_TRANSFORM_TASK,US_12_01_CAR_TRANSFORM_TASK,_TASKS,US_12,North Carolina,truckDelivery,,310,3800,US_12_01_CAR_TRANSFORM_TASK_DESC,TASKS\nUS_12_01_PRIVATE_ORDER_TASK,US_12_01_PRIVATE_ORDER_TASK,_TASKS,US_12,North Carolina,cargoDelivery,4\u00d7 Forklift Caravan Container 2,1300,10100,US_12_01_PRIVATE_ORDER_TASK_DESC,TASKS\nUS_12_01_SPARE_PARTS_TASK,US_12_01_SPARE_PARTS_TASK,_TASKS,US_12,North Carolina,cargoDelivery,4\u00d7 Vehicles Spare Parts,320,3550,US_12_01_SPARE_PARTS_TASK_DESC,TASKS\nUS_12_01_DELIVERY,US_12_01_DELIVERY,_CONTESTS,US_12,North Carolina,cargoDelivery,1\u00d7 Radioctive,100,550,US_12_01_DELIVERY_DESC,CONTESTS\nUS_12_01_TRIAL,US_12_01_TRIAL,_CONTESTS,US_12,North Carolina,exploration,,120,700,US_12_01_TRIAL_DESC,CONTESTS\nUS_12_02_ENGINEERING_WORKS_CONTRACT,US_12_02_ENGINEERING_WORKS_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Service Spare Parts,1390,15550,US_12_02_ENGINEERING_WORKS_CONTRACT_DESC,CONTRACTS\nUS_12_02_STATOR_FOR_START_CONTRACT,US_12_02_STATOR_FOR_START_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,,1050,14950,US_12_02_STATOR_FOR_START_CONTRACT_DESC,CONTRACTS\nUS_12_02_DELIVERY_TOOLS_CONTRACT,US_12_02_DELIVERY_TOOLS_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,2\u00d7 Vehicles Spare Parts,880,9650,US_12_02_DELIVERY_TOOLS_CONTRACT_DESC,CONTRACTS\nUS_12_02_FEMM_DELIVERY_CONTRACT,US_12_02_FEMM_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,,350,3500,US_12_02_FEMM_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_02_LARGE_DELIVERY_CONTRACT,US_12_02_LARGE_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Logs Short; 4\u00d7 Logs Medium; 1\u00d7 Logs Long,2700,32600,US_12_02_LARGE_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_02_PUMPS_DELIVERY_CONTRACT,US_12_02_PUMPS_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Npp Pump,2590,28350,US_12_02_PUMPS_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_02_START_WORK_CONTRACT,US_12_02_START_WORK_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Metal Planks; 2\u00d7 Metal Roll; 4\u00d7 Bags,2810,23900,US_12_02_START_WORK_CONTRACT_DESC,CONTRACTS\nUS_12_02_HELP_SUN_CONTRACT,US_12_02_HELP_SUN_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,6\u00d7 Solar Panel,1730,19950,US_12_02_HELP_SUN_CONTRACT_DESC,CONTRACTS\nUS_12_02_WATER_FOR_THE_NEED_CONTRACT,US_12_02_WATER_FOR_THE_NEED_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2700\u00d7 Water; 1900\u00d7 Water; 1800\u00d7 Water,2370,17550,US_12_02_WATER_FOR_THE_NEED_CONTRACT_DESC,CONTRACTS\nUS_12_02_ATOM_CONTRACT,US_12_02_ATOM_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Bags; 2\u00d7 Metal Planks; 3\u00d7 Metal Roll,1910,16500,US_12_02_ATOM_CONTRACT_DESC,CONTRACTS\nUS_12_02_SAWMILL_REPAIR_CONTRACT,US_12_02_SAWMILL_REPAIR_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Metal Planks; 4\u00d7 Metal Roll,1800,15450,US_12_02_SAWMILL_REPAIR_CONTRACT_DESC,CONTRACTS\nUS_12_02_RESOURCES_DELIVERY_CONTRACT,US_12_02_RESOURCES_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Wooden Planks Medium; 3\u00d7 Wooden Planks; 2\u00d7 Vehicles Spare Parts,1180,13450,US_12_02_RESOURCES_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_02_RESTORATION_OF_RAILWAY_CONTRACT,US_12_02_RESTORATION_OF_RAILWAY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Wooden Planks Medium; 1\u00d7 Railway,1140,13150,US_12_02_RESTORATION_OF_RAILWAY_CONTRACT_DESC,CONTRACTS\nUS_12_02_BOILER_FOR_FARM_TASK,US_12_02_BOILER_FOR_FARM_TASK,_TASKS,US_12,North Carolina,truckDelivery,1\u00d7 Boiler,1260,13700,US_12_02_BOILER_FOR_FARM_TASK_DESC,TASKS\nUS_12_02_DANGEROUS_BEND_TASK,US_12_02_DANGEROUS_BEND_TASK,_TASKS,US_12,North Carolina,truckDelivery,1\u00d7 Pipe Large,1070,6550,US_12_02_DANGEROUS_BEND_TASK_DESC,TASKS\nUS_12_02_SAD_RIDE_TASK,US_12_02_SAD_RIDE_TASK,_TASKS,US_12,North Carolina,truckDelivery,,460,4950,US_12_02_SAD_RIDE_TASK_DESC,TASKS\nUS_12_02_FALL_FROM_BRIDGE_TASK,US_12_02_FALL_FROM_BRIDGE_TASK,_TASKS,US_12,North Carolina,truckDelivery,,450,4400,US_12_02_FALL_FROM_BRIDGE_TASK_DESC,TASKS\nUS_12_02_NIGHT_ERROR_TASK,US_12_02_NIGHT_ERROR_TASK,_TASKS,US_12,North Carolina,truckDelivery,,410,3750,US_12_02_NIGHT_ERROR_TASK_DESC,TASKS\nUS_12_02_REPAIR_TRAILER_TASK,US_12_02_REPAIR_TRAILER_TASK,_TASKS,US_12,North Carolina,truckDelivery,,350,3050,US_12_02_REPAIR_TRAILER_TASK_DESC,TASKS\nUS_12_02_STUCK_MUD_TASK,US_12_02_STUCK_MUD_TASK,_TASKS,US_12,North Carolina,truckDelivery,,340,2850,US_12_02_STUCK_MUD_TASK_DESC,TASKS\nUS_12_02_HELPING_NEIGHBORS_TASK,US_12_02_HELPING_NEIGHBORS_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Wooden Planks Medium; 3\u00d7 Wooden Planks; 1\u00d7 Wooden Planks Long; 2\u00d7 Wooden Planks,1060,11600,US_12_02_HELPING_NEIGHBORS_TASK_DESK,TASKS\nUS_12_02_BLOCK_ON_THE_ROAD_TASK,US_12_02_BLOCK_ON_THE_ROAD_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Wooden Planks Medium,460,5600,US_12_02_BLOCK_ON_THE_ROAD_TASK_DESC,TASKS\nUS_12_02_RESCUE_WORKSHOP_TASK,US_12_02_RESCUE_WORKSHOP_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Service Spare Parts,450,5150,US_12_02_RESCUE_WORKSHOP_TASK_DESC,TASKS\nUS_12_02_BEST_FOR_NPP_TASK,US_12_02_BEST_FOR_NPP_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Container Small Special,540,4500,US_12_02_BEST_FOR_NPP_TASK_DESC,TASKS\nUS_12_02_FALLEN_HAY_TASK,US_12_02_FALLEN_HAY_TASK,_TASKS,US_12,North Carolina,cargoDelivery,3\u00d7 Stack,390,2900,US_12_02_FALLEN_HAY_TASK_DESC,TASKS\nUS_12_02_MUD_TRACK_CONTEST,US_12_02_MUD_TRACK_CONTEST,_CONTESTS,US_12,North Carolina,exploration,,120,1600,US_12_02_MUD_TRACK_CONTEST_DESC,CONTESTS\nUS_12_02_MOUNTAIN_RIDE_CONTEST,US_12_02_MOUNTAIN_RIDE_CONTEST,_CONTESTS,US_12,North Carolina,exploration,,140,1400,US_12_02_MOUNTAIN_RIDE_CONTEST_DESC,CONTESTS\nUS_12_03_COOLING_TOWERS_01_CONTRACT,US_12_03_COOLING_TOWERS_01_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Blocks; 2\u00d7 Pipes Small; 1\u00d7 Container Large,1790,17450,US_12_03_COOLING_TOWERS_01_DESC,CONTRACTS\nUS_12_03_CONSTRUCTION_BRIDGE_01_CONTRACT,US_12_03_CONSTRUCTION_BRIDGE_01_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Blocks,1020,10650,US_12_03_CONSTRUCTION_BRIDGE_01_DESC,CONTRACTS\nUS_12_03_CONSTRUCTION_WATCHPOINT_01_CONTRACT,US_12_03_CONSTRUCTION_WATCHPOINT_01_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,1\u00d7 Wooden Planks; 1\u00d7 Service Spare Parts,620,6400,US_12_03_CONSTRUCTION_WATCHPOINT_01_DESC,CONTRACTS\nUS_12_03_CONSTRUCTION_GARAGE_01_CONTRACT,US_12_03_CONSTRUCTION_GARAGE_01_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Metal Roll,690,5950,US_12_03_CONSTRUCTION_GARAGE_01_DESC,CONTRACTS\nUS_12_03_SCOUT_01_TSK,US_12_03_SCOUT_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,700,8450,US_12_03_SCOUT_01_DESC,TASKS\nUS_12_03_REPAIR_TOWER_01_TSK,US_12_03_REPAIR_TOWER_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,630,7200,US_12_03_REPAIR_TOWER_01_DESC,TASKS\nUS_12_03_TRAILER_PARK_01_TSK,US_12_03_TRAILER_PARK_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,630,7200,US_12_03_TRAILER_PARK_01_DESC,TASKS\nUS_12_03_TRAILER_01_TSK,US_12_03_TRAILER_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,560,6050,US_12_03_TRAILER_01_DESC,TASKS\nUS_12_03_SCOUT_TRAILER_01_TSK,US_12_03_SCOUT_TRAILER_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,540,5600,US_12_03_SCOUT_TRAILER_01_DESC,TASKS\nUS_12_03_TRUCK_01_TSK,US_12_03_TRUCK_01_TSK,_TASKS,US_12,North Carolina,truckDelivery,,470,4800,US_12_03_TRUCK_01_DESC,TASKS\nUS_12_03_WATER_HOUSE_01_TSK,US_12_03_WATER_HOUSE_01_TSK,_TASKS,US_12,North Carolina,cargoDelivery,1300\u00d7 Water,1190,10550,US_12_03_WATER_HOUSE_01_DESC,TASKS\nUS_12_03_ECO_ORDER_01_TSK,US_12_03_ECO_ORDER_01_TSK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Service Spare Parts Special,560,6400,US_12_03_ECO_ORDER_01_DESC,TASKS\nUS_12_03_FISHERS_01_TSK,US_12_03_FISHERS_01_TSK,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Service Spare Parts,360,4250,US_12_03_FISHERS_01_DESC,TASKS\nUS_12_03_CONT_RACE_01,US_12_03_CONT_RACE_01,_CONTESTS,US_12,North Carolina,exploration,,50,1200,US_12_03_CONT_RACE_01_DESC,CONTESTS\nUS_12_03_COOLING_TOWERS_02_CONTRACT,US_12_03_COOLING_TOWERS_02_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Concrete Slab; 2\u00d7 Container Small; 2\u00d7 Metal Planks,2190,18950,US_12_03_COOLING_TOWERS_02_DESC,CONTRACTS\nUS_12_03_CONSTRUCTION_BRIDGE_02_CONTRACT,US_12_03_CONSTRUCTION_BRIDGE_02_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Blocks,1020,10500,US_12_03_CONSTRUCTION_BRIDGE_02_DESC,CONTRACTS\nUS_12_03_CONSTRUCTION_GARAGE_02_CONTRACT,US_12_03_CONSTRUCTION_GARAGE_02_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Bags; 2\u00d7 Service Spare Parts,950,7950,US_12_03_CONSTRUCTION_GARAGE_02_DESC,CONTRACTS\nUS_12_03_SCOUT_TRAILER_02_TSK,US_12_03_SCOUT_TRAILER_02_TSK,_TASKS,US_12,North Carolina,truckDelivery,,610,7250,US_12_03_SCOUT_TRAILER_02_DESC,TASKS\nUS_12_03_SCOUT_02_TSK,US_12_03_SCOUT_02_TSK,_TASKS,US_12,North Carolina,truckDelivery,,510,5150,US_12_03_SCOUT_02_DESC,TASKS\nUS_12_03_WATER_HOUSE_02_TSK,US_12_03_WATER_HOUSE_02_TSK,_TASKS,US_12,North Carolina,cargoDelivery,1500\u00d7 Water,1250,11650,US_12_03_WATER_HOUSE_02_DESC,TASKS\nUS_12_03_CONT_RACE_02,US_12_03_CONT_RACE_02,_CONTESTS,US_12,North Carolina,exploration,,80,1400,US_12_03_CONT_RACE_02_DESC,CONTESTS\nUS_12_03_REPAIR_CRANE_CONTRACT,US_12_03_REPAIR_CRANE_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,,510,5400,US_12_03_REPAIR_CRANE_DESC,CONTRACTS\nUS_12_03_FARM_WATER_CONTRACT,US_12_03_FARM_WATER_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2700\u00d7 Water; 1500\u00d7 Water; 1500\u00d7 Water,2300,18250,US_12_03_FARM_WATER_DESC,CONTRACTS\nUS_12_03_TRAILER_SHOP_CONTRACT,US_12_03_TRAILER_SHOP_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Metal Roll; 1\u00d7 Metal Planks; 2\u00d7 Vehicles Spare Parts,1030,10150,US_12_03_TRAILER_SHOP_DESC,CONTRACTS\nUS_12_03_REPAIR_DAM_CONTRACT,US_12_03_REPAIR_DAM_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,1\u00d7 Container Small; 1\u00d7 Container Small,790,7700,US_12_03_REPAIR_DAM_DESC,CONTRACTS\nUS_12_03_BUNKER_ORDER_CONTRACT,US_12_03_BUNKER_ORDER_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Rubbish,780,7500,US_12_03_BUNKER_ORDER_DESC,CONTRACTS\nUS_12_03_SEISMO_CHECK_CONTRACT,US_12_03_SEISMO_CHECK_CONTRACT,_CONTRACTS,US_12,North Carolina,exploration,,360,6200,US_12_03_SEISMO_CHECK_DESC,CONTRACTS\nUS_12_03_TRAILER_ENERGY_TSK,US_12_03_TRAILER_ENERGY_TSK,_TASKS,US_12,North Carolina,truckDelivery,,520,5600,US_12_03_TRAILER_ENERGY_DESC,TASKS\nUS_12_04_FESTIVAL_PREPARING_01_TASK,US_12_04_FESTIVAL_PREPARING_01_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Logs Short; 1\u00d7 Service Spare Parts,570,6600,US_12_04_FESTIVAL_PREPARING_01_TASK_DESC,TASKS\nUS_12_04_FESTIVAL_PREPARING_02_TASK,US_12_04_FESTIVAL_PREPARING_02_TASK,_TASKS,US_12,North Carolina,truckDelivery,2\u00d7 Cellulose,970,11750,US_12_04_FESTIVAL_PREPARING_02_TASK_DESC,TASKS\nUS_12_04_FACTORY_OIL_CONTRACT,US_12_04_FACTORY_OIL_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,1\u00d7 Forklift Caravan Container 2; 2\u00d7 Metal Planks,2110,19750,US_12_04_FACTORY_OIL_CONTRACT_DESC,CONTRACTS\nUS_12_04_FACTORY_METAL_CONTRACT,US_12_04_FACTORY_METAL_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,2\u00d7 Wooden Planks; 2\u00d7 Blocks,1180,13600,US_12_04_FACTORY_METAL_CONTRACT_DESC,CONTRACTS\nUS_12_04_ANTENNA_BUILDING_CONTRACT,US_12_04_ANTENNA_BUILDING_CONTRACT,_CONTRACTS,US_12,North Carolina,truckDelivery,2\u00d7 Metal Roll; 1\u00d7 Metal Planks,1040,10400,US_12_04_ANTENNA_BUILDING_CONTRACT_DESC,CONTRACTS\nUS_12_04_LOGS_DELIVERY_CONTRACT,US_12_04_LOGS_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Logs Short; 2\u00d7 Logs Medium; 3\u00d7 Logs Short; 1\u00d7 Logs Long; 1\u00d7 Logs Short; 1\u00d7 Logs Medium; 1\u00d7 Logs Long,2360,25250,US_12_04_LOGS_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_04_OIL_DELIVERY_CONTRACT,US_12_04_OIL_DELIVERY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Barrels Oil,1170,14450,US_12_04_OIL_DELIVERY_CONTRACT_DESC,CONTRACTS\nUS_12_04_RAILWAY_STATION_REPAIR_CONTRACT,US_12_04_RAILWAY_STATION_REPAIR_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Forklift Caravan Container 2; 2\u00d7 Metal Planks; 4\u00d7 Service Spare Parts,1850,13150,US_12_04_RAILWAY_STATION_REPAIR_CONTRACT_DESC,CONTRACTS\nUS_12_04_COOLING_TOWER_TRASH_CONTRACT,US_12_04_COOLING_TOWER_TRASH_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,3\u00d7 Rubbish,1220,13100,US_12_04_COOLING_TOWER_TRASH_CONTRACT_DESC,CONTRACTS\nUS_12_04_RAILWAY_REPAIR_CONTRACT,US_12_04_RAILWAY_REPAIR_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Service Spare Parts,840,10250,US_12_04_RAILWAY_REPAIR_CONTRACT_DESC,CONTRACTS\nUS_12_04_OBSERVATORY_CONTRACT,US_12_04_OBSERVATORY_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,3\u00d7 Solar Panel; 3\u00d7 Bricks,1010,9950,US_12_04_OBSERVATORY_CONTRACT_DESC,CONTRACTS\nUS_12_04_REMOVAL_RADIOACTIVE_CONTRACT,US_12_04_REMOVAL_RADIOACTIVE_CONTRACT,_CONTRACTS,US_12,North Carolina,cargoDelivery,4\u00d7 Radioctive,800,9850,US_12_04_REMOVAL_RADIOACTIVE_CONTRACT_DESC,CONTRACTS\nUS_12_04_CITY_SCOUT_CONTRACT,US_12_04_CITY_SCOUT_CONTRACT,_CONTRACTS,US_12,North Carolina,exploration,,30,500,US_12_04_CITY_SCOUT_CONTRACT_DESC,CONTRACTS\nUS_12_04_OIL_PROBLEMS_TASK,US_12_04_OIL_PROBLEMS_TASK,_TASKS,US_12,North Carolina,truckDelivery,,890,10500,US_12_04_OIL_PROBLEMS_TASK_DESC,TASKS\nUS_12_04_SLIPPERY_ROAD_TASK,US_12_04_SLIPPERY_ROAD_TASK,_TASKS,US_12,North Carolina,truckDelivery,,420,5100,US_12_04_SLIPPERY_ROAD_TASK_DESC,TASKS\nUS_12_04_FOREST_VECHICLE_TASK,US_12_04_FOREST_VECHICLE_TASK,_TASKS,US_12,North Carolina,truckDelivery,1\u00d7 Vehicles Spare Parts,440,4300,US_12_04_FOREST_VECHICLE_TASK_DESC,TASKS\nUS_12_04_LOGS_PROBLEM_TASK,US_12_04_LOGS_PROBLEM_TASK,_TASKS,US_12,North Carolina,truckDelivery,,330,3450,US_12_04_LOGS_PROBLEM_TASK_DESC,TASKS\nUS_12_04_UNFINISHED_BUSINESS_TASK,US_12_04_UNFINISHED_BUSINESS_TASK,_TASKS,US_12,North Carolina,truckDelivery,,300,3400,US_12_04_UNFINISHED_BUSINESS_TASK_DESC,TASKS\nUS_12_04_USEFUL_SEMITRAILER_TASK,US_12_04_USEFUL_SEMITRAILER_TASK,_TASKS,US_12,North Carolina,truckDelivery,,310,3200,US_12_04_USEFUL_SEMITRAILER_TASK_DESC,TASKS\nUS_12_04_BAD_PARKING_PLACE_TASK,US_12_04_BAD_PARKING_PLACE_TASK,_TASKS,US_12,North Carolina,truckDelivery,,310,3050,US_12_04_BAD_PARKING_PLACE_TASK_DESC,TASKS\nUS_12_04_WRONG_TURN_TASK,US_12_04_WRONG_TURN_TASK,_TASKS,US_12,North Carolina,truckDelivery,,320,2550,US_12_04_WRONG_TURN_TASK_DESC,TASKS\nUS_12_04_HIGHWAY_TASK,US_12_04_HIGHWAY_TASK,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Forklift Caravan Container 2; 2\u00d7 Vehicles Spare Parts,990,11450,US_12_04_HIGHWAY_TASK_DESC,TASKS\nUS_12_04_SMALL_HELP_TASK,US_12_04_SMALL_HELP_TASK,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Wooden Planks; 1\u00d7 Bricks,400,5250,US_12_04_SMALL_HELP_TASK_DESC,TASKS\nUS_12_04_TRAIL_TO_THE_TOWER_TASK,US_12_04_TRAIL_TO_THE_TOWER_TASK,_TASKS,US_12,North Carolina,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Wooden Planks,410,4400,US_12_04_TRAIL_TO_THE_TOWER_TASK_DESC,TASKS\nUS_12_04_OBSTACLE_ON_THE_ROAD_TASK,US_12_04_OBSTACLE_ON_THE_ROAD_TASK,_TASKS,US_12,North Carolina,cargoDelivery,2\u00d7 Service Spare Parts,240,2750,US_12_04_OBSTACLE_ON_THE_ROAD_TASK_DESC,TASKS\nUS_12_04_COASTLINE_RACE_CONTEST,US_12_04_COASTLINE_RACE_CONTEST,_CONTESTS,US_12,North Carolina,exploration,,450,900,US_12_04_COASTLINE_RACE_CONTEST_DESC,CONTESTS\nRU_13_01_SUPPLY_CAREER_01_CONTRACT,RU_13_01_SUPPLY_CAREER_01_CONTRACT,_CONTRACTS,RU_13,Almaty,truckDelivery,,440,4300,RU_13_01_SUPPLY_CAREER_01_CONTRACT_DESC,CONTRACTS\nRU_13_01_EXPORT_SHALE_01_CONTRACT,RU_13_01_EXPORT_SHALE_01_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Stone Block,2010,17100,RU_13_01_EXPORT_SHALE_01_CONTRACT_DESC,CONTRACTS\nRU_13_01_CONSTRUCTION_RIBBONS_01_CONTRACT,RU_13_01_CONSTRUCTION_RIBBONS_01_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts; 2\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,1640,14350,RU_13_01_CONSTRUCTION_RIBBONS_01_CONTRACT_DESC,CONTRACTS\nRU_13_01_AERODROM_01_CONTRACT,RU_13_01_AERODROM_01_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Bags; 2\u00d7 Rubbish,1090,9050,RU_13_01_AERODROM_01_CONTRACT_DESC,CONTRACTS\nRU_13_01_SEISMO_01_CONTRACT,RU_13_01_SEISMO_01_CONTRACT,_CONTRACTS,RU_13,Almaty,exploration,,400,6950,RU_13_01_SEISMO_01_CONTRACT_DESC,CONTRACTS\nRU_13_01_HELP_ADMINISTRATION_01,RU_13_01_HELP_ADMINISTRATION_01,_TASKS,RU_13,Almaty,truckDelivery,,760,8700,RU_13_01_HELP_ADMINISTRATION_01_DESC,TASKS\nRU_13_01_SUPPLY_CAREER_02_CONTRACT,RU_13_01_SUPPLY_CAREER_02_CONTRACT,_CONTRACTS,RU_13,Almaty,truckDelivery,,480,5300,RU_13_01_SUPPLY_CAREER_02_CONTRACT_DESC,CONTRACTS\nRU_13_01_EXPORT_SHALE_02_CONTRACT,RU_13_01_EXPORT_SHALE_02_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Stone Block,2130,19250,RU_13_01_EXPORT_SHALE_02_CONTRACT_DESC,CONTRACTS\nRU_13_01_AERODROM_02_CONTRACT,RU_13_01_AERODROM_02_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Metal Roll; 2\u00d7 Container Small Special,950,8500,RU_13_01_AERODROM_02_CONTRACT_DESC,CONTRACTS\nRU_13_01_CONSTRUCTION_RIBBONS_02_CONTRACT,RU_13_01_CONSTRUCTION_RIBBONS_02_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Metal Planks,780,6450,RU_13_01_CONSTRUCTION_RIBBONS_02_CONTRACT_DESC,CONTRACTS\nRU_13_01_SEISMO_02_CONTRACT,RU_13_01_SEISMO_02_CONTRACT,_CONTRACTS,RU_13,Almaty,exploration,,130,2150,RU_13_01_SEISMO_02_CONTRACT_DESC,CONTRACTS\nRU_13_01_HELP_ADMINISTRATION_02,RU_13_01_HELP_ADMINISTRATION_02,_TASKS,RU_13,Almaty,cargoDelivery,4\u00d7 Bricks,450,4700,RU_13_01_HELP_ADMINISTRATION_02_DESC,TASKS\nRU_13_01_SUPPLY_CAREER_03_CONTRACT,RU_13_01_SUPPLY_CAREER_03_CONTRACT,_CONTRACTS,RU_13,Almaty,truckDelivery,1\u00d7 Tank Wagon; 4\u00d7 Barrels Oil,790,9800,RU_13_01_SUPPLY_CAREER_03_CONTRACT_DESC,CONTRACTS\nRU_13_01_SEISMO_03_CONTRACT,RU_13_01_SEISMO_03_CONTRACT,_CONTRACTS,RU_13,Almaty,exploration,,130,2250,RU_13_01_SEISMO_03_CONTRACT_DESC,CONTRACTS\nRU_13_01_HELP_ADMINISTRATION_03,RU_13_01_HELP_ADMINISTRATION_03,_TASKS,RU_13,Almaty,cargoDelivery,4\u00d7 Blocks,900,9750,RU_13_01_HELP_ADMINISTRATION_03_DESC,TASKS\nRU_13_01_SUPPLY_CAREER_04_CONTRACT,RU_13_01_SUPPLY_CAREER_04_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,1\u00d7 Container Large Drilling; 5\u00d7 Service Spare Parts Special,1670,18100,RU_13_01_SUPPLY_CAREER_04_CONTRACT_DESC,CONTRACTS\nRU_13_01_EXPORT_SILVER_CONTRACT,RU_13_01_EXPORT_SILVER_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,8\u00d7 Silver,1700,15950,RU_13_01_EXPORT_SILVER_CONTRACT_DESC,CONTRACTS\nRU_13_01_DELIVERY_TROLLEY_CONTRACT,RU_13_01_DELIVERY_TROLLEY_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Minecart,1750,15600,RU_13_01_DELIVERY_TROLLEY_CONTRACT_DESC,CONTRACTS\nRU_13_01_REPAIR_SILVER_CONTRACT,RU_13_01_REPAIR_SILVER_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,4\u00d7 Vehicles Spare Parts; 4\u00d7 Service Spare Parts,800,7750,RU_13_01_REPAIR_SILVER_CONTRACT_DESC,CONTRACTS\nRU_13_01_LOGISTICS_CENTER_CONTRACT,RU_13_01_LOGISTICS_CENTER_CONTRACT,_CONTRACTS,RU_13,Almaty,cargoDelivery,8\u00d7 Service Spare Parts,690,7650,RU_13_01_LOGISTICS_CENTER_CONTRACT_DESC,CONTRACTS\nRU_13_01_GEO_EXPLORATION_CONTRACT,RU_13_01_GEO_EXPLORATION_CONTRACT,_CONTRACTS,RU_13,Almaty,exploration,,310,5400,RU_13_01_GEO_EXPLORATION_CONTRACT_DESC,CONTRACTS\nRU_13_01_NOT_A_ROLLER_COASTER,RU_13_01_NOT_A_ROLLER_COASTER,_TASKS,RU_13,Almaty,truckDelivery,1\u00d7 Container Large,1420,15050,RU_13_01_NOT_A_ROLLER_COASTER_DESC,TASKS\nRU_13_01_WATER_SHARES_STONE_TASK,RU_13_01_WATER_SHARES_STONE_TASK,_TASKS,RU_13,Almaty,truckDelivery,1800\u00d7 Water; 2000\u00d7 Water,1420,14000,RU_13_01_WATER_SHARES_STONE_TASK_DESC,TASKS\nRU_13_01_HELP_BORAT,RU_13_01_HELP_BORAT,_TASKS,RU_13,Almaty,truckDelivery,1\u00d7 Forklift Caravan Container 2,1060,8650,RU_13_01_HELP_BORAT_DESC,TASKS\nRU_13_01_LOGS_FOR_HOUSE,RU_13_01_LOGS_FOR_HOUSE,_TASKS,RU_13,Almaty,truckDelivery,1\u00d7 Logs Medium,720,7200,RU_13_01_LOGS_FOR_HOUSE_DESC,TASKS\nRU_13_01_SHEPHERD_TROUBLE,RU_13_01_SHEPHERD_TROUBLE,_TASKS,RU_13,Almaty,truckDelivery,,420,5100,RU_13_01_SHEPHERD_TROUBLE_DESC,TASKS\nRU_13_01_GEO_RADAR,RU_13_01_GEO_RADAR,_TASKS,RU_13,Almaty,truckDelivery,,320,3600,RU_13_01_GEO_RADAR_DESC,TASKS\nRU_13_01_TOURIST_LIST,RU_13_01_TOURIST_LIST,_TASKS,RU_13,Almaty,truckDelivery,,260,2650,RU_13_01_TOURIST_LIST_DESC,TASKS\nRU_13_01_TRASH_EXPORT_TASK,RU_13_01_TRASH_EXPORT_TASK,_TASKS,RU_13,Almaty,cargoDelivery,3\u00d7 Rubbish,1730,24400,RU_13_01_TRASH_EXPORT_TASK_DESC,TASKS\nRU_13_01_HUMANITARIAN_ASSISTANCE_FUEL,RU_13_01_HUMANITARIAN_ASSISTANCE_FUEL,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil; 1\u00d7 Barrels Oil,1310,14700,RU_13_01_HUMANITARIAN_ASSISTANCE_FUEL_DESC,TASKS\nRU_13_01_RELIABLE_SUPPORT_TASK,RU_13_01_RELIABLE_SUPPORT_TASK,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Wooden Planks Medium; 2\u00d7 Wooden Planks; 2\u00d7 Metal Planks,1250,13500,RU_13_01_RELIABLE_SUPPORT_TASK_DESC,TASKS\nRU_13_01_HUMANITARIAN_ASSISTANCE_WATER,RU_13_01_HUMANITARIAN_ASSISTANCE_WATER,_TASKS,RU_13,Almaty,cargoDelivery,750\u00d7 Water; 900\u00d7 Water,1090,12500,RU_13_01_HUMANITARIAN_ASSISTANCE_WATER_DESC,TASKS\nRU_13_01_WATER_TOWER_REPAIR,RU_13_01_WATER_TOWER_REPAIR,_TASKS,RU_13,Almaty,cargoDelivery,2\u00d7 Metal Roll; 1\u00d7 Pipes Small,960,10800,RU_13_01_WATER_TOWER_REPAIR_DESC,TASKS\nRU_13_01_HUMANITARIAN_ASSISTANCE_FOOD,RU_13_01_HUMANITARIAN_ASSISTANCE_FOOD,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Potato; 1\u00d7 Potato; 1\u00d7 Potato,890,9200,RU_13_01_HUMANITARIAN_ASSISTANCE_FOOD_DESC,TASKS\nRU_13_01_AVIATION_FUEL,RU_13_01_AVIATION_FUEL,_TASKS,RU_13,Almaty,cargoDelivery,3\u00d7 Barrels Oil; 3\u00d7 Barrels Oil,920,9100,RU_13_01_AVIATION_FUEL_TASK_DESC,TASKS\nRU_13_01_LOGS_PROBLEM,RU_13_01_LOGS_PROBLEM,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Logs Long,670,6900,RU_13_01_LOGS_PROBLEM_TASK_DESC,TASKS\nRU_13_01_EASTERN_BRIDGE,RU_13_01_EASTERN_BRIDGE,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Wooden Planks Medium,600,6850,RU_13_01_EASTERN_BRIDGE_DESK,TASKS\nRU_13_01_CANYON_BLOCKAGE,RU_13_01_CANYON_BLOCKAGE,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,680,6750,RU_13_01_CANYON_BLOCKAGE_DESK,TASKS\nRU_13_01_GREAT_DEAL,RU_13_01_GREAT_DEAL,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Service Spare Parts,480,6700,RU_13_01_GREAT_DEAL_DESC,TASKS\nRU_13_01_QUARRY_BLOCKAGE,RU_13_01_QUARRY_BLOCKAGE,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Metal Planks; 2\u00d7 Service Spare Parts,660,6350,RU_13_01_QUARRY_BLOCKAGE_DESK,TASKS\nRU_13_01_NORTHERN_BRIDGE,RU_13_01_NORTHERN_BRIDGE,_TASKS,RU_13,Almaty,cargoDelivery,1\u00d7 Wooden Planks Medium; 1\u00d7 Logs Short,400,4050,RU_13_01_NORTHERN_BRIDGE_DESK,TASKS\nRU_13_01_RIBBONS_ROAD_SCOUT_TASK,RU_13_01_RIBBONS_ROAD_SCOUT_TASK,_TASKS,RU_13,Almaty,exploration,,210,3650,RU_13_01_RIBBONS_ROAD_SCOUT_TASK_DESC,TASKS\nRU_13_01_TOWER_ROAD_SCOUT_TASK,RU_13_01_TOWER_ROAD_SCOUT_TASK,_TASKS,RU_13,Almaty,exploration,,90,1450,RU_13_01_TOWER_ROAD_SCOUT_TASK_DESC,TASKS\nRU_13_01_SOUTH_ROAD_SCOUT_TASK,RU_13_01_SOUTH_ROAD_SCOUT_TASK,_TASKS,RU_13,Almaty,exploration,,60,1000,RU_13_01_SOUTH_ROAD_SCOUT_TASK_DESC,TASKS\nRU_13_01_IN_SEARCH_OF_AIDAHAR_CONTEST,RU_13_01_IN_SEARCH_OF_AIDAHAR_CONTEST,_CONTESTS,RU_13,Almaty,exploration,,450,900,RU_13_01_IN_SEARCH_OF_AIDAHAR_CONTEST_DESC,CONTESTS\nRU_13_01_MOUNTAIN_CLIMBING_CONTEST,RU_13_01_MOUNTAIN_CLIMBING_CONTEST,_CONTESTS,RU_13,Almaty,exploration,,400,800,RU_13_01_MOUNTAIN_CLIMBING_CONTEST_DESC,CONTESTS\nUS_14_01_WAREHOUSE_PROBLEMS_CONTRACT,US_14_01_WAREHOUSE_PROBLEMS_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,1\u00d7 Rubbish; 4\u00d7 Metal Planks; 3\u00d7 Metal Roll,3150,32050,US_14_01_WAREHOUSE_PROBLEMS_DESC,CONTRACTS\nUS_14_01_EMERGENCY_HELP_CONTRACT,US_14_01_EMERGENCY_HELP_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,3\u00d7 Metal Planks; 2\u00d7 Metal Roll; 2\u00d7 Wooden Planks Medium,2790,24100,US_14_01_EMERGENCY_HELP_DESC,CONTRACTS\nUS_14_01_PIPES_DELIVERY_CONTRACT,US_14_01_PIPES_DELIVERY_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,1\u00d7 Pipes Small; 1\u00d7 Pipes Medium; 4\u00d7 Bags,2140,21200,US_14_01_PIPES_DELIVERY_DESC,CONTRACTS\nUS_14_01_PROBLEM_IN_THE_PORT_CONTRACT,US_14_01_PROBLEM_IN_THE_PORT_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,6\u00d7 Vehicles Spare Parts,1120,11100,US_14_01_PROBLEM_IN_THE_PORT_DESC,CONTRACTS\nUS_14_01_FARM_SOYA_CONTRACT,US_14_01_FARM_SOYA_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,4\u00d7 Fertilizer,1100,10200,US_14_01_FARM_SOYA_DESC,CONTRACTS\nUS_14_01_STARTING_WORK_CONTRACT,US_14_01_STARTING_WORK_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,4\u00d7 Vehicles Spare Parts; 6\u00d7 Metal Roll,1340,9850,US_14_01_STARTING_WORK_DESC,CONTRACTS\nUS_14_01_HELP_FROM_THE_PAST_CONTRACT,US_14_01_HELP_FROM_THE_PAST_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,,810,7800,US_14_01_HELP_FROM_THE_PAST_DESC,CONTRACTS\nUS_14_01_SOY_DELIVERY_CONTRACT,US_14_01_SOY_DELIVERY_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,3\u00d7 Soyabean; 3\u00d7 Soyabean,2580,31050,US_14_01_SOY_DELIVERY_DESC,CONTRACTS\nUS_14_01_CASTLE_UNDER_LOCK_CONTRACT,US_14_01_CASTLE_UNDER_LOCK_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,6\u00d7 Stack; 4\u00d7 Potato; 2\u00d7 Wooden Planks; 2\u00d7 Logs Medium,2590,26000,US_14_01_CASTLE_UNDER_LOCK_DESC,CONTRACTS\nUS_14_01_HAY_COLLECTION_CONTRACT,US_14_01_HAY_COLLECTION_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,8\u00d7 Stack,1520,15350,US_14_01_HAY_COLLECTION_DESC,CONTRACTS\nUS_14_01_LOST_LARGE_CARGO_CONTRACT,US_14_01_LOST_LARGE_CARGO_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,2\u00d7 Container Small Special,940,10450,US_14_01_LOST_LARGE_CARGO_DESC,CONTRACTS\nUS_14_01_REPAIR_AREA_CONTRACT,US_14_01_REPAIR_AREA_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,3\u00d7 Barrels; 4\u00d7 Vehicles Spare Parts,970,8350,US_14_01_REPAIR_AREA_DESC,CONTRACTS\nUS_14_01_FARM_POTATO_CONTRACT,US_14_01_FARM_POTATO_CONTRACT,_CONTRACTS,US_14,Austria,exploration,,860,9400,US_14_01_FARM_POTATO_DESC,CONTRACTS\nUS_14_01_SCOUT_FARM_CONTRACT,US_14_01_SCOUT_FARM_CONTRACT,_CONTRACTS,US_14,Austria,exploration,,370,6350,US_14_01_SCOUT_FARM_DESC,CONTRACTS\nUS_14_01_FORTRESS_CENTURY_TASK,US_14_01_FORTRESS_CENTURY_TASK,_TASKS,US_14,Austria,truckDelivery,4\u00d7 Bricks,760,6550,US_14_01_FORTRESS_CENTURY_DESC,TASKS\nUS_14_01_PROBLEM_WITH_TURNING_TASK,US_14_01_PROBLEM_WITH_TURNING_TASK,_TASKS,US_14,Austria,truckDelivery,,670,6350,US_14_01_PROBLEM_WITH_TURNING_DESC,TASKS\nUS_14_01_REPAIR_PROBLEM_TASK,US_14_01_REPAIR_PROBLEM_TASK,_TASKS,US_14,Austria,truckDelivery,,550,6250,US_14_01_REPAIR_PROBLEM_DESC,TASKS\nUS_14_01_LET_THERE_LIGHT_TASK,US_14_01_LET_THERE_LIGHT_TASK,_TASKS,US_14,Austria,truckDelivery,2\u00d7 Vehicles Spare Parts,580,5450,US_14_01_LET_THERE_LIGHT_DESC,TASKS\nUS_14_01_ELEMENTAL_POWER_TASK,US_14_01_ELEMENTAL_POWER_TASK,_TASKS,US_14,Austria,truckDelivery,,400,4000,US_14_01_ELEMENTAL_POWER_DESC,TASKS\nUS_14_01_MAKE_HASTE_SLOWLY_TASK,US_14_01_MAKE_HASTE_SLOWLY_TASK,_TASKS,US_14,Austria,truckDelivery,,370,3400,US_14_01_MAKE_HASTE_SLOWLY_DESC,TASKS\nUS_14_01_SHARP_TURN_TASK,US_14_01_SHARP_TURN_TASK,_TASKS,US_14,Austria,truckDelivery,,360,3300,US_14_01_SHARP_TURN_DESC,TASKS\nUS_14_01_FUEL_PROBLEMS_TASK,US_14_01_FUEL_PROBLEMS_TASK,_TASKS,US_14,Austria,truckDelivery,,330,3050,US_14_01_FUEL_PROBLEMS_DESC,TASKS\nUS_14_01_PRODUCTION_ERROR_TASK,US_14_01_PRODUCTION_ERROR_TASK,_TASKS,US_14,Austria,cargoDelivery,2\u00d7 Wooden Planks Long,620,7150,US_14_01_PRODUCTION_ERROR_DESC,TASKS\nUS_14_01_WATER_TREATMENTS_TASK,US_14_01_WATER_TREATMENTS_TASK,_TASKS,US_14,Austria,cargoDelivery,4800\u00d7 Water,610,6400,US_14_01_WATER_TREATMENTS_DESC,TASKS\nUS_14_01_UNPLANNED_UNLOADING_TASK,US_14_01_UNPLANNED_UNLOADING_TASK,_TASKS,US_14,Austria,cargoDelivery,2\u00d7 Cellulose,500,5100,US_14_01_UNPLANNED_UNLOADING_DESC,TASKS\nUS_14_01_DIRT_FROM_PAST_TASK,US_14_01_DIRT_FROM_PAST_TASK,_TASKS,US_14,Austria,cargoDelivery,3\u00d7 Barrels Chemicals,570,4250,US_14_01_DIRT_FROM_PAST_DESC,TASKS\nUS_14_01_LOSS_IN_RIVER_TASK,US_14_01_LOSS_IN_RIVER_TASK,_TASKS,US_14,Austria,cargoDelivery,3\u00d7 Vehicles Spare Parts,390,3450,US_14_01_LOSS_IN_RIVER_DESC,TASKS\nUS_14_01_URGENT_PHOTOS_CONTEST,US_14_01_URGENT_PHOTOS_CONTEST,_CONTESTS,US_14,Austria,exploration,,70,1400,US_14_01_URGENT_PHOTOS_DESC,CONTESTS\nUS_14_01_CITY_RACING_CONTEST,US_14_01_CITY_RACING_CONTEST,_CONTESTS,US_14,Austria,exploration,,50,1000,US_14_01_CITY_RACING_DESC,CONTESTS\nUS_14_02_DELIVERY_LOG_01_CONTRACT,US_14_02_DELIVERY_LOG_01_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,2\u00d7 Logs Short,970,10500,US_14_02_DELIVERY_LOG_01_DESC,CONTRACTS\nUS_14_02_BUILD_CASTLE_01_CONTRACT,US_14_02_BUILD_CASTLE_01_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,2\u00d7 Bricks; 1\u00d7 Metal Planks; 2\u00d7 Bags; 2\u00d7 Container Small,2220,21800,US_14_02_BUILD_CASTLE_01_DESC,CONTRACTS\nUS_14_02_FORREST_BURNED_01_CONTRACT,US_14_02_FORREST_BURNED_01_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,700\u00d7 Water; 800\u00d7 Water,960,8550,US_14_02_FORREST_BURNED_01_DESC,CONTRACTS\nUS_14_02_TRAILER_01_TASK,US_14_02_TRAILER_01_TASK,_TASKS,US_14,Austria,truckDelivery,,600,6650,US_14_02_TRAILER_01_DESC,TASKS\nUS_14_02_SCOUT_TRAILER_01_TASK,US_14_02_SCOUT_TRAILER_01_TASK,_TASKS,US_14,Austria,truckDelivery,,460,4550,US_14_02_SCOUT_TRAILER_01_DESC,TASKS\nUS_14_02_TRUCK_01_TASK,US_14_02_TRUCK_01_TASK,_TASKS,US_14,Austria,truckDelivery,,380,3600,US_14_02_TRUCK_01_DESC,TASKS\nUS_14_02_BUILD_BRIDGE_01_TASK,US_14_02_BUILD_BRIDGE_01_TASK,_TASKS,US_14,Austria,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Concrete Slab,1480,11150,US_14_02_BUILD_BRIDGE_01_DESC,TASKS\nUS_14_02_DELIVERY_HELP_01_TASK,US_14_02_DELIVERY_HELP_01_TASK,_TASKS,US_14,Austria,cargoDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Crate Large,810,8200,US_14_02_DELIVERY_HELP_01_DESC,TASKS\nUS_14_02_ROCKS_01_TASK,US_14_02_ROCKS_01_TASK,_TASKS,US_14,Austria,cargoDelivery,1\u00d7 Blocks; 1\u00d7 Service Spare Parts,320,2450,US_14_02_ROCKS_01_DESC,TASKS\nUS_14_02_FORREST_BURNED_02_CONTRACT,US_14_02_FORREST_BURNED_02_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,10\u00d7 Burnt Logs,2310,22200,US_14_02_FORREST_BURNED_02_DESC,CONTRACTS\nUS_14_02_DELIVERY_LOG_02_CONTRACT,US_14_02_DELIVERY_LOG_02_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,1\u00d7 Logs Long,730,9050,US_14_02_DELIVERY_LOG_02_DESC,CONTRACTS\nUS_14_02_TRAILER_02_TASK,US_14_02_TRAILER_02_TASK,_TASKS,US_14,Austria,truckDelivery,4\u00d7 Stack,1270,14500,US_14_02_TRAILER_02_DESC,TASKS\nUS_14_02_SCOUT_TRAILER_02_TASK,US_14_02_SCOUT_TRAILER_02_TASK,_TASKS,US_14,Austria,truckDelivery,,340,2850,US_14_02_SCOUT_TRAILER_02_DESC,TASKS\nUS_14_02_TRUCK_02_TASK,US_14_02_TRUCK_02_TASK,_TASKS,US_14,Austria,truckDelivery,,310,2050,US_14_02_TRUCK_02_DESC,TASKS\nUS_14_02_DELIVERY_HELP_02_TASK,US_14_02_DELIVERY_HELP_02_TASK,_TASKS,US_14,Austria,cargoDelivery,1\u00d7 Logs Short; 1\u00d7 Crate Large,1040,11750,US_14_02_DELIVERY_HELP_02_DESC,TASKS\nUS_14_02_ROCKS_02_TASK,US_14_02_ROCKS_02_TASK,_TASKS,US_14,Austria,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Bags,530,5100,US_14_02_ROCKS_02_DESC,TASKS\nUS_14_02_DELIVERY_LOG_03_CONTRACT,US_14_02_DELIVERY_LOG_03_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Long,950,11500,US_14_02_DELIVERY_LOG_03_DESC,CONTRACTS\nUS_14_02_TRUCK_03_TASK,US_14_02_TRUCK_03_TASK,_TASKS,US_14,Austria,truckDelivery,,390,3450,US_14_02_TRUCK_03_DESC,TASKS\nUS_14_02_TRUCK_04_TASK,US_14_02_TRUCK_04_TASK,_TASKS,US_14,Austria,truckDelivery,,430,4450,US_14_02_TRUCK_04_DESC,TASKS\nUS_14_02_SERVICE_HOUSE_CONTRACT,US_14_02_SERVICE_HOUSE_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,3\u00d7 Vehicles Spare Parts; 1\u00d7 Wooden Planks; 1\u00d7 Barrels,1620,18500,US_14_02_SERVICE_HOUSE_DESC,CONTRACTS\nUS_14_02_FORREST_HELP_CONTRACT,US_14_02_FORREST_HELP_CONTRACT,_CONTRACTS,US_14,Austria,truckDelivery,2\u00d7 Forklift Caravan Container 2,2030,15750,US_14_02_FORREST_HELP_DESC,CONTRACTS\nUS_14_02_DELIVERY_POTATO_CONTRACT,US_14_02_DELIVERY_POTATO_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,1\u00d7 Potato; 1\u00d7 Potato; 1\u00d7 Potato; 1\u00d7 Potato; 1\u00d7 Potato; 1\u00d7 Potato,2140,21550,US_14_02_DELIVERY_POTATO_DESC,CONTRACTS\nUS_14_02_HELP_FARM_CONTRACT,US_14_02_HELP_FARM_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,3\u00d7 Bags 2; 3\u00d7 Rubbish,1860,18450,US_14_02_HELP_FARM_DESC,CONTRACTS\nUS_14_02_SOLAR_PANEL_CONTRACT,US_14_02_SOLAR_PANEL_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts; 1\u00d7 Solar Panel; 1\u00d7 Solar Panel,1510,18050,US_14_02_SOLAR_PANEL_DESC,CONTRACTS\nUS_14_02_FARM_POTATO_CONTRACT,US_14_02_FARM_POTATO_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,2\u00d7 Fertilizer; 1\u00d7 Wooden Planks,1100,11500,US_14_02_FARM_POTATO_DESC,CONTRACTS\nUS_14_02_FARM_SOYA_CONTRACT,US_14_02_FARM_SOYA_CONTRACT,_CONTRACTS,US_14,Austria,cargoDelivery,2\u00d7 Fertilizer,980,9500,US_14_02_FARM_SOYA_DESC,CONTRACTS\nUS_14_02_SCOUTING_EXPLORER_CONTRACT,US_14_02_SCOUTING_EXPLORER_CONTRACT,_CONTRACTS,US_14,Austria,exploration,,500,8550,US_14_02_SCOUTING_EXPLORER_DESC,CONTRACTS\nUS_14_02_SCOUTING_CASTLES_CONTRACT,US_14_02_SCOUTING_CASTLES_CONTRACT,_CONTRACTS,US_14,Austria,exploration,,470,8150,US_14_02_SCOUTING_CASTLES_DESC,CONTRACTS\nUS_14_02_WATER_TANK_TASK,US_14_02_WATER_TANK_TASK,_TASKS,US_14,Austria,cargoDelivery,600\u00d7 Water; 600\u00d7 Water; 600\u00d7 Water,1140,9150,US_14_02_WATER_TANK_DESC,TASKS\nUS_14_02_CAMP_RUNNER_TASK,US_14_02_CAMP_RUNNER_TASK,_TASKS,US_14,Austria,cargoDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Barrels; 1\u00d7 Crate Large,820,7000,US_14_02_CAMP_RUNNER_DESC,TASKS\nUS_14_02_SCOUT_RIVER_TASK,US_14_02_SCOUT_RIVER_TASK,_TASKS,US_14,Austria,exploration,,270,4550,US_14_02_SCOUT_RIVER_DESC,TASKS\nUS_14_02_DELIVERY_CONTEST,US_14_02_DELIVERY_CONTEST,_CONTESTS,US_14,Austria,cargoDelivery,1\u00d7 Stack,280,2600,US_14_02_DELIVERY_CONT_DESC,CONTESTS\nUS_14_02_RACING_CONTEST,US_14_02_RACING_CONTEST,_CONTESTS,US_14,Austria,exploration,,220,3900,US_14_02_RACING_DESC,CONTESTS\nUS_15_01_PLATFORM_CONSTRUCTION_01_CONTRACT,US_15_01_PLATFORM_CONSTRUCTION_01_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,,1120,15750,US_15_01_PLATFORM_CONSTRUCTION_01_CONTRACT_DESC,CONTRACTS\nUS_15_01_LIGHTHOUSE_SUPPLY_01_CONTRACT,US_15_01_LIGHTHOUSE_SUPPLY_01_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,,420,4300,US_15_01_LIGHTHOUSE_SUPPLY_01_CONTRACT_DESC,CONTRACTS\nUS_15_01_BUILDING_BRIDGE_01_CONTRACT,US_15_01_BUILDING_BRIDGE_01_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,4\u00d7 Metal Planks; 6\u00d7 Bags,2610,20950,US_15_01_BUILDING_BRIDGE_01_CONTRACT_DESC,CONTRACTS\nUS_15_01_PIPELINE_01_CONTRACT,US_15_01_PIPELINE_01_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,1\u00d7 Pipe Water,750,5650,US_15_01_PIPELINE_01_CONTRACT_DESC,CONTRACTS\nUS_15_01_SCOUTING_SHORE_CONTRACT_01,US_15_01_SCOUTING_SHORE_CONTRACT_01,_CONTRACTS,US_15,Quebec,exploration,,300,5250,US_15_01_SCOUTING_SHORE_CONTRACT_01_DESC,CONTRACTS\nUS_15_01_TRAILER_DELIVERY_01_TASK,US_15_01_TRAILER_DELIVERY_01_TASK,_TASKS,US_15,Quebec,truckDelivery,,530,5750,US_15_01_TRAILER_DELIVERY_01_TASK_DESC,TASKS\nUS_15_01_TRACK_DELIVERY_01_TASK,US_15_01_TRACK_DELIVERY_01_TASK,_TASKS,US_15,Quebec,truckDelivery,,490,5100,US_15_01_TRACK_DELIVERY_01_TASK_DESC,TASKS\nUS_15_01_HOUSE_BUILDING_01_TASK,US_15_01_HOUSE_BUILDING_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,4\u00d7 Wooden Planks Medium,970,13300,US_15_01_HOUSE_BUILDING_01_TASK_DESC,TASKS\nUS_15_01_CARGO_PORT_01_TASK,US_15_01_CARGO_PORT_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Metal Planks; 4\u00d7 Metal Roll,1500,13000,US_15_01_CARGO_PORT_01_TASK_DESC,TASKS\nUS_15_01_PIPES_01_TASK,US_15_01_PIPES_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Pipe Large,1560,11650,US_15_01_PIPES_01_TASK_DESC,TASKS\nUS_15_01_ROAD_BLOCK_01_TASK,US_15_01_ROAD_BLOCK_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,1\u00d7 Wooden Planks; 1\u00d7 Crate Large,840,10200,US_15_01_ROAD_BLOCK_01_TASK_DESC,TASKS\nUS_15_01_LOST_CARGO_01_TASK,US_15_01_LOST_CARGO_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,3\u00d7 Barrels Chemicals,660,8100,US_15_01_LOST_CARGO_01_TASK_DESC,TASKS\nUS_15_01_PANTOON_BRIDGE_01_TASK,US_15_01_PANTOON_BRIDGE_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Metal Roll; 2\u00d7 Wooden Planks Medium,740,6050,US_15_01_PANTOON_BRIDGE_01_TASK_DESC,TASKS\nUS_15_01_CARGO_DELIVERY_01_TASK,US_15_01_CARGO_DELIVERY_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,3\u00d7 Barrels,590,5850,US_15_01_CARGO_DELIVERY_01_TASK_DESC,TASKS\nUS_15_01_CONTEST_01,US_15_01_CONTEST_01,_CONTESTS,US_15,Quebec,exploration,,150,2700,US_15_01_CONTEST_01_DESC,CONTESTS\nUS_15_01_PLATFORM_CONSTRUCTION_02_CONTRACT,US_15_01_PLATFORM_CONSTRUCTION_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Container Large Drilling; 1\u00d7 Big Drills,2410,29900,US_15_01_PLATFORM_CONSTRUCTION_02_CONTRACT_DESC,CONTRACTS\nUS_15_01_BUILDING_BRIDGE_02_CONTRACT,US_15_01_BUILDING_BRIDGE_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Container Large; 4\u00d7 Blocks; 2\u00d7 Metal Planks,2540,21950,US_15_01_BUILDING_BRIDGE_02_CONTRACT_DESC,CONTRACTS\nUS_15_01_SHIPBOARD_SUPPLY_02_CONTRACT,US_15_01_SHIPBOARD_SUPPLY_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,8\u00d7 Service Spare Parts,1170,13450,US_15_01_SHIPBOARD_SUPPLY_02_CONTRACT_DESC,CONTRACTS\nUS_15_01_LIGHTHOUSE_SUPPLY_02_CONTRACT,US_15_01_LIGHTHOUSE_SUPPLY_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,8\u00d7 Service Spare Parts,780,8450,US_15_01_LIGHTHOUSE_SUPPLY_02_CONTRACT_DESC,CONTRACTS\nUS_15_01_SCOUTING_SHORE_CONTRACT_02,US_15_01_SCOUTING_SHORE_CONTRACT_02,_CONTRACTS,US_15,Quebec,cargoDelivery,1\u00d7 Crate Large; 1\u00d7 Crate Large; 1\u00d7 Crate Large,870,7900,US_15_01_SCOUTING_SHORE_CONTRACT_02_DESC,CONTRACTS\nUS_15_01_PIPELINE_02_CONTRACT,US_15_01_PIPELINE_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,1\u00d7 Pipe Water,890,7100,US_15_01_PIPELINE_02_CONTRACT_DESC,CONTRACTS\nUS_15_01_TRAILER_DELIVERY_02_TASK,US_15_01_TRAILER_DELIVERY_02_TASK,_TASKS,US_15,Quebec,truckDelivery,,560,6400,US_15_01_TRAILER_DELIVERY_02_TASK_DESC,TASKS\nUS_15_01_ROAD_BLOCK_02_TASK,US_15_01_ROAD_BLOCK_02_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Wooden Planks Medium,390,4550,US_15_01_ROAD_BLOCK_02_TASK_DESC,TASKS\nUS_15_01_CARGO_DELIVERY_02_TASK,US_15_01_CARGO_DELIVERY_02_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Bags,490,4250,US_15_01_CARGO_DELIVERY_02_TASK_DESC,TASKS\nUS_15_01_CONTEST_02,US_15_01_CONTEST_02,_CONTESTS,US_15,Quebec,cargoDelivery,1\u00d7 Silver; 1\u00d7 Gold,900,8000,US_15_01_CONTEST_02_DESC,CONTESTS\nUS_15_01_BUILDING_BRIDGE_03_CONTRACT,US_15_01_BUILDING_BRIDGE_03_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,3\u00d7 Barrels; 2\u00d7 Metal Planks,1550,14950,US_15_01_BUILDING_BRIDGE_03_CONTRACT_DESC,CONTRACTS\nUS_15_01_PLATFORM_CONSTRUCTION_03_CONTRACT,US_15_01_PLATFORM_CONSTRUCTION_03_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,4\u00d7 Container Small,1430,14400,US_15_01_PLATFORM_CONSTRUCTION_03_CONTRACT_DESC,CONTRACTS\nUS_15_01_ROAD_BLOCK_03_TASK,US_15_01_ROAD_BLOCK_03_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Service Spare Parts,740,9650,US_15_01_ROAD_BLOCK_03_TASK_DESC,TASKS\nUS_15_01_PLATFORM_CONSTRUCTION_04_CONTRACT,US_15_01_PLATFORM_CONSTRUCTION_04_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,,870,11350,US_15_01_PLATFORM_CONSTRUCTION_04_CONTRACT_DESC,CONTRACTS\nUS_15_01_CABLE_TRAILER_CONTRACT,US_15_01_CABLE_TRAILER_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,,900,11850,US_15_01_CABLE_TRAILER_CONTRACT_DESC,CONTRACTS\nUS_15_01_CONSTRUCTION_FACTORY_CONTRACT,US_15_01_CONSTRUCTION_FACTORY_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,4\u00d7 Bricks; 2\u00d7 Concrete Slab,1650,15850,US_15_01_CONSTRUCTION_FACTORY_CONTRACT_DESC,CONTRACTS\nUS_15_01_ROAD_ISLAND_CONTRACT,US_15_01_ROAD_ISLAND_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Wooden Planks Medium; 2\u00d7 Metal Roll,1250,14500,US_15_01_ROAD_ISLAND_CONTRACT_DESC,CONTRACTS\nUS_15_01_CONSTRUCTION_CLIMBERS_CONTRACT,US_15_01_CONSTRUCTION_CLIMBERS_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,3\u00d7 Barrels Oil; 2\u00d7 Vehicles Spare Parts,1050,7400,US_15_01_CONSTRUCTION_CLIMBERS_CONTRACT_DESC,CONTRACTS\nUS_15_01_RESTORE_ROUTE_CONTRACT,US_15_01_RESTORE_ROUTE_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Wooden Planks Medium; 2\u00d7 Wooden Planks Medium,560,5200,US_15_01_RESTORE_ROUTE_CONTRACT_DESC,CONTRACTS\nUS_15_01_METEO_DELIVERY_TASK,US_15_01_METEO_DELIVERY_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Bags; 2\u00d7 Service Spare Parts,1030,9750,US_15_01_METEO_DELIVERY_TASK_DESC,TASKS\nUS_15_01_SHIPYARD_SUPPLY_TASK,US_15_01_SHIPYARD_SUPPLY_TASK,_TASKS,US_15,Quebec,cargoDelivery,8\u00d7 Barrels,1050,8850,US_15_01_SHIPYARD_SUPPLY_TASK_DESC,TASKS\nUS_15_01_CRASHED_CARGO_TASK,US_15_01_CRASHED_CARGO_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Container Small Special,710,6950,US_15_01_CRASHED_CARGO_TASK_DESC,TASKS\nUS_15_01_TRASH_TASK,US_15_01_TRASH_TASK,_TASKS,US_15,Quebec,cargoDelivery,2\u00d7 Rubbish,630,5600,US_15_01_TRASH_TASK_DESC,TASKS\nUS_15_02_HIGHWAY_01_CONTRACT,US_15_02_HIGHWAY_01_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,2\u00d7 Metal Planks; 3\u00d7 Concrete Slab,2360,19900,US_15_02_HIGHWAY_01_DESC,CONTRACTS\nUS_15_02_HELP_TRUCK_01_TASK,US_15_02_HELP_TRUCK_01_TASK,_TASKS,US_15,Quebec,truckDelivery,,440,4300,US_15_02_HELP_TRUCK_01_DESC,TASKS\nUS_15_02_BLOCKAGE_01_TASK,US_15_02_BLOCKAGE_01_TASK,_TASKS,US_15,Quebec,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Service Spare Parts,970,10000,US_15_02_BLOCKAGE_01_DESC,TASKS\nUS_15_02_HIGHWAY_02_CONTRACT,US_15_02_HIGHWAY_02_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Container Small; 3\u00d7 Vehicles Spare Parts; 2\u00d7 Wooden Planks Medium; 3\u00d7 Bags,1650,13000,US_15_02_HIGHWAY_02_DESC,CONTRACTS\nUS_15_02_HELP_TRUCK_02_TASK,US_15_02_HELP_TRUCK_02_TASK,_TASKS,US_15,Quebec,truckDelivery,,380,3650,US_15_02_HELP_TRUCK_02_DESC,TASKS\nUS_15_02_BLOCKAGE_02_TASK,US_15_02_BLOCKAGE_02_TASK,_TASKS,US_15,Quebec,cargoDelivery,1\u00d7 Vehicles Spare Parts; 2\u00d7 Crate Large,1040,11200,US_15_02_BLOCKAGE_02_DESC,TASKS\nUS_15_02_HELP_TRUCK_03_TASK,US_15_02_HELP_TRUCK_03_TASK,_TASKS,US_15,Quebec,truckDelivery,1\u00d7 Wooden Planks; 1\u00d7 Wooden Planks Medium,740,6200,US_15_02_HELP_TRUCK_03_DESC,TASKS\nUS_15_02_HELP_TRUCK_04_TASK,US_15_02_HELP_TRUCK_04_TASK,_TASKS,US_15,Quebec,truckDelivery,,560,5950,US_15_02_HELP_TRUCK_04_DESC,TASKS\nUS_15_02_WINTER_NEED_CONTRACT,US_15_02_WINTER_NEED_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,2\u00d7 Logs Short; 4\u00d7 Vehicles Spare Parts,1840,21350,US_15_02_WINTER_NEED_DESC,CONTRACTS\nUS_15_02_UNDER_ICE_CONTRACT,US_15_02_UNDER_ICE_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts; 1\u00d7 Vehicles Spare Parts,1860,20600,US_15_02_UNDER_ICE_DESC,CONTRACTS\nUS_15_02_TUNNEL_CONTRACT,US_15_02_TUNNEL_CONTRACT,_CONTRACTS,US_15,Quebec,truckDelivery,1\u00d7 Container Large Drilling,1460,14200,US_15_02_TUNNEL_DESC,CONTRACTS\nUS_15_02_SUPPLY_PARK_CONTRACT,US_15_02_SUPPLY_PARK_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,4\u00d7 Wooden Planks; 2\u00d7 Logs Short; 2\u00d7 Barrels Oil; 2\u00d7 Barrels Oil,1900,21252,US_15_02_SUPPLY_PARK_DESC,CONTRACTS\nUS_15_02_QUARRY_DELIVERY_CONTRACT,US_15_02_QUARRY_DELIVERY_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Service Spare Parts Special; 2\u00d7 Container Large,1790,15750,US_15_02_QUARRY_DELIVERY_DESC,CONTRACTS\nUS_15_02_FISH_FESTIVAL_CONTRACT,US_15_02_FISH_FESTIVAL_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Logs Medium,1260,14400,US_15_02_FISH_FESTIVAL_DESC,CONTRACTS\nUS_15_02_SUPPLY_PARAGLIDING_CONTRACT,US_15_02_SUPPLY_PARAGLIDING_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,3\u00d7 Vehicles Spare Parts; 5\u00d7 Barrels Oil,1300,13250,US_15_02_SUPPLY_PARAGLIDING_DESC,CONTRACTS\nUS_15_02_SUPPLY_LOG_CONTRACT,US_15_02_SUPPLY_LOG_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Vehicles Spare Parts,1010,12050,US_15_02_SUPPLY_LOG_DESC,CONTRACTS\nUS_15_02_PIPE_CASES_CONTRACT,US_15_02_PIPE_CASES_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Pipe Large,980,9500,US_15_02_PIPE_CASES_DESC,CONTRACTS\nUS_15_02_WASTE_RECYCLING_CONTRACT,US_15_02_WASTE_RECYCLING_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,2\u00d7 Rubbish,840,7200,US_15_02_WASTE_RECYCLING_DESC,CONTRACTS\nUS_15_02_PIPELINE_CONTRACT,US_15_02_PIPELINE_CONTRACT,_CONTRACTS,US_15,Quebec,cargoDelivery,1\u00d7 Pipe Water,370,4850,US_15_02_PIPELINE_DESC,CONTRACTS\nUS_15_02_BRICKS_TASK,US_15_02_BRICKS_TASK,_TASKS,US_15,Quebec,truckDelivery,2\u00d7 Bricks,960,9900,US_15_02_BRICKS_DESC,TASKS\nUS_15_02_OLD_RESOURCE_TASK,US_15_02_OLD_RESOURCE_TASK,_TASKS,US_15,Quebec,truckDelivery,,620,7050,US_15_02_OLD_RESOURCE_DESC,TASKS\nUS_15_02_STUCK_ICE_TASK,US_15_02_STUCK_ICE_TASK,_TASKS,US_15,Quebec,truckDelivery,,500,4050,US_15_02_STUCK_ICE_DESC,TASKS\nUS_15_02_SUPPLY_HELP_TASK,US_15_02_SUPPLY_HELP_TASK,_TASKS,US_15,Quebec,cargoDelivery,1\u00d7 Crate Large; 1\u00d7 Crate Large; 1\u00d7 Crate Large,1220,12850,US_15_02_SUPPLY_HELP_DESC,TASKS\nUS_15_02_CHEMISTRY_TASK,US_15_02_CHEMISTRY_TASK,_TASKS,US_15,Quebec,cargoDelivery,3\u00d7 Barrels Chemicals,900,8600,US_15_02_CHEMISTRY_DESC,TASKS\nUS_15_02_CONTEST_MOUNTAIN,US_15_02_CONTEST_MOUNTAIN,_CONTESTS,US_15,Quebec,exploration,,120,2400,US_15_02_CONTEST_MOUNTAIN_DESC,CONTESTS\nUS_15_02_CONTEST_RACING,US_15_02_CONTEST_RACING,_CONTESTS,US_15,Quebec,exploration,,100,1500,US_15_02_CONTEST_RACING_DESC,CONTESTS\nUS_16_01_BUILD_BRIDGE_01_CONTRACT,US_16_01_BUILD_BRIDGE_01_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,3\u00d7 Concrete Slab; 1\u00d7 Container Large,3030,32000,US_16_01_BUILD_BRIDGE_01_DESC,CONTRACTS\nUS_16_01_SCOUT_TRAILER_01_TASK,US_16_01_SCOUT_TRAILER_01_TASK,_TASKS,US_16,Washington,truckDelivery,,1040,12600,US_16_01_SCOUT_TRAILER_01_DESC,TASKS\nUS_16_01_DELIVERY_TRUCK_01_TASK,US_16_01_DELIVERY_TRUCK_01_TASK,_TASKS,US_16,Washington,truckDelivery,,910,10300,US_16_01_DELIVERY_TRUCK_01_DESC,TASKS\nUS_16_01_DELIVERY_SCOUT_01_TASK,US_16_01_DELIVERY_SCOUT_01_TASK,_TASKS,US_16,Washington,truckDelivery,,860,9400,US_16_01_DELIVERY_SCOUT_01_DESC,TASKS\nUS_16_01_DELIVERY_TRAILER_01_TASK,US_16_01_DELIVERY_TRAILER_01_TASK,_TASKS,US_16,Washington,truckDelivery,,440,4600,US_16_01_DELIVERY_TRAILER_01_DESC,TASKS\nUS_16_01_ROCKS_01_TASK,US_16_01_ROCKS_01_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Metal Planks,1130,8550,US_16_01_ROCKS_01_DESC,TASKS\nUS_16_01_WOODEN_BRIDGE_01_TASK,US_16_01_WOODEN_BRIDGE_01_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Wooden Planks,470,6000,US_16_01_WOODEN_BRIDGE_01_DESC,TASKS\nUS_16_01_DELIVERY_TRUCK_02_TASK,US_16_01_DELIVERY_TRUCK_02_TASK,_TASKS,US_16,Washington,truckDelivery,,670,7950,US_16_01_DELIVERY_TRUCK_02_DESC,TASKS\nUS_16_01_DELIVERY_TRAILER_02_TASK,US_16_01_DELIVERY_TRAILER_02_TASK,_TASKS,US_16,Washington,truckDelivery,,510,5550,US_16_01_DELIVERY_TRAILER_02_DESC,TASKS\nUS_16_01_DELIVERY_SCOUT_02_TASK,US_16_01_DELIVERY_SCOUT_02_TASK,_TASKS,US_16,Washington,truckDelivery,,390,3450,US_16_01_DELIVERY_SCOUT_02_DESC,TASKS\nUS_16_01_ROCKS_02_TASK,US_16_01_ROCKS_02_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Bags; 2\u00d7 Crate Large,1060,8550,US_16_01_ROCKS_02_DESC,TASKS\nUS_16_01_TUNNEL_RESTORATION_CONTRACT,US_16_01_TUNNEL_RESTORATION_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,4\u00d7 Bags; 3\u00d7 Concrete Slab,4910,56400,US_16_01_TUNNEL_RESTORATION_DESC,CONTRACTS\nUS_16_01_FACTORY_DELIVERY_CONTRACT,US_16_01_FACTORY_DELIVERY_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Container Small,1960,22450,US_16_01_FACTORY_DELIVERY_DESC,CONTRACTS\nUS_16_01_RECOVERY_GATE_CONTRACT,US_16_01_RECOVERY_GATE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,3\u00d7 Bags; 2\u00d7 Bags 2,1980,19600,US_16_01_RECOVERY_GATE_DESC,CONTRACTS\nUS_16_01_LOST_DELIVERY_CONTRACT,US_16_01_LOST_DELIVERY_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,,1210,12600,US_16_01_LOST_DELIVERY_DESC,CONTRACTS\nUS_16_01_WOODEN_BATCH_CONTRACT,US_16_01_WOODEN_BATCH_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Logs Short,3200,47550,US_16_01_WOODEN_BATCH_DESC,CONTRACTS\nUS_16_01_HELP_DELIVERY_CONTRACT,US_16_01_HELP_DELIVERY_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Service Spare Parts; 1100\u00d7 Water; 3\u00d7 Crate Large,2260,25100,US_16_01_HELP_DELIVERY_DESC,CONTRACTS\nUS_16_01_CITY_DELIVERY_CONTRACT,US_16_01_CITY_DELIVERY_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Bags 2; 2\u00d7 Bricks; 2\u00d7 Wooden Planks; 1\u00d7 Pipes Small,2070,23450,US_16_01_CITY_DELIVERY_DESC,CONTRACTS\nUS_16_01_WAREHOUSE_RUIN_CONTRACT,US_16_01_WAREHOUSE_RUIN_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Junk Open; 2\u00d7 Junk Closed,2280,23100,US_16_01_WAREHOUSE_RUIN_DESC,CONTRACTS\nUS_16_01_BUILD_HANGAR_CONTRACT,US_16_01_BUILD_HANGAR_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,3\u00d7 Metal Roll; 4\u00d7 Wooden Planks,1640,19600,US_16_01_BUILD_HANGAR_DESC,CONTRACTS\nUS_16_01_LIGHTHOUSE_CONTRACT,US_16_01_LIGHTHOUSE_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Logs Short,1460,19450,US_16_01_LIGHTHOUSE_DESC,CONTRACTS\nUS_16_01_BUILD_HOUSE_CONTRACT,US_16_01_BUILD_HOUSE_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Logs Medium; 2\u00d7 Wooden Planks; 1\u00d7 Container Small,1530,19100,US_16_01_BUILD_HOUSE_DESC,CONTRACTS\nUS_16_01_WOODS_CONTRACT,US_16_01_WOODS_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Barrels; 1\u00d7 Solar Panel; 1\u00d7 Service Spare Parts,1120,13300,US_16_01_WOODS_DESC,CONTRACTS\nUS_16_01_SCOUTING_CONTRACT,US_16_01_SCOUTING_CONTRACT,_CONTRACTS,US_16,Washington,exploration,,900,15700,US_16_01_SCOUTING_DESC,CONTRACTS\nUS_16_01_LITTLE_WATER_TASK,US_16_01_LITTLE_WATER_TASK,_TASKS,US_16,Washington,truckDelivery,2\u00d7 Vehicles Spare Parts,670,5550,US_16_01_LITTLE_WATER_DESC,TASKS\nUS_16_01_FISHERS_ORDER_TASK,US_16_01_FISHERS_ORDER_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Crate Large; 1\u00d7 Service Spare Parts,1040,10850,US_16_01_FISHERS_ORDER_DESC,TASKS\nUS_16_01_DELIVERY_WOOD_TASK,US_16_01_DELIVERY_WOOD_TASK,_TASKS,US_16,Washington,cargoDelivery,1\u00d7 Barrels; 1\u00d7 Vehicles Spare Parts,720,7950,US_16_01_DELIVERY_WOOD_DESC,TASKS\nUS_16_01_DELIVERY_HELP_TASK,US_16_01_DELIVERY_HELP_TASK,_TASKS,US_16,Washington,cargoDelivery,1\u00d7 Wooden Planks; 1\u00d7 Wooden Planks; 1\u00d7 Wooden Planks,580,7600,US_16_01_DELIVERY_HELP_DESC,TASKS\nUS_16_01_DELIVERY_ISLAND_TASK,US_16_01_DELIVERY_ISLAND_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Fertilizer,570,5150,US_16_01_DELIVERY_ISLAND_DESC,TASKS\nUS_16_01_EXPLORE_CONTEST,US_16_01_EXPLORE_CONTEST,_CONTESTS,US_16,Washington,exploration,,150,3000,US_16_01_EXPLORE_DESC,CONTESTS\nUS_16_01_DRIVE_CONTEST,US_16_01_DRIVE_CONTEST,_CONTESTS,US_16,Washington,exploration,,150,2700,US_16_01_DRIVE_DESC,CONTESTS\nUS_16_02_RAILWAY_BRIDGE_01_CONTRACT,US_16_02_RAILWAY_BRIDGE_01_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,4\u00d7 Vehicles Spare Parts,960,11150,US_16_02_RAILWAY_BRIDGE_01_DESC,CONTRACTS\nUS_16_02_RAILWAY_BRIDGE_02_CONTRACT,US_16_02_RAILWAY_BRIDGE_02_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Junk Open; 2\u00d7 Junk Closed; 2\u00d7 Railway; 2\u00d7 Metal Planks; 2\u00d7 Bags,3880,40850,US_16_02_RAILWAY_BRIDGE_02_DESC,CONTRACTS\nUS_16_02_WINTER_UPDATE_CONTRACT,US_16_02_WINTER_UPDATE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,1\u00d7 Boiler; 1600\u00d7 Water,2050,19000,US_16_02_WINTER_UPDATE_DESC,CONTRACTS\nUS_16_02_GIANT_NEED_CONTRACT,US_16_02_GIANT_NEED_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Bags 2; 1\u00d7 Metal Planks,1530,14500,US_16_02_GIANT_NEED_DESC,CONTRACTS\nUS_16_02_RESTORATION_SAWMILL_CONTRACT,US_16_02_RESTORATION_SAWMILL_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Metal Planks; 4\u00d7 Metal Roll,1180,13550,US_16_02_RESTORATION_SAWMILL_DESC,CONTRACTS\nUS_16_02_BROKEN_GATE_TASK,US_16_02_BROKEN_GATE_TASK,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Logs Medium,610,6700,US_16_02_BROKEN_GATE_DESC,CONTRACTS\nUS_16_02_YEAR_AHEAD_CONTRACT,US_16_02_YEAR_AHEAD_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Logs Short; 1\u00d7 Logs Medium; 1\u00d7 Logs Long; 2\u00d7 Logs Medium,3100,40100,US_16_02_YEAR_AHEAD_DESC,CONTRACTS\nUS_16_02_SCOUT_CARE_CONTRACT,US_16_02_SCOUT_CARE_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Wooden Planks Medium; 2\u00d7 Logs Medium; 2\u00d7 Container Small; 2\u00d7 Service Spare Parts,2340,27100,US_16_02_SCOUT_CARE_DESC,CONTRACTS\nUS_16_02_WATER_PIPES_CONTRACT,US_16_02_WATER_PIPES_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Pipes Small; 2\u00d7 Pipes Medium; 2\u00d7 Blocks,2090,24500,US_16_02_WATER_PIPES_DESC,CONTRACTS\nUS_16_02_HELPING_FOREST_SERVICE_CONTRACT,US_16_02_HELPING_FOREST_SERVICE_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks Long; 4\u00d7 Bags; 4\u00d7 Bags 2,2370,23700,US_16_02_HELPING_FOREST_SERVICE_DESC,CONTRACTS\nUS_16_02_WINTER_SUPPLIES_CONTRACT,US_16_02_WINTER_SUPPLIES_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1600\u00d7 Water; 2\u00d7 Logs Short; 3\u00d7 Bags 2,1650,15900,US_16_02_WINTER_SUPPLIES_DESC,CONTRACTS\nUS_16_02_BUOY_BUOY_CONTRACT,US_16_02_BUOY_BUOY_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy,1370,13100,US_16_02_BUOY_BUOY_DESC,CONTRACTS\nUS_16_02_WOOD_BLOCK_CONTRACT,US_16_02_WOOD_BLOCK_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts,870,9750,US_16_02_WOOD_BLOCK_DESC,CONTRACTS\nUS_16_02_BRIDGE_REPAIR_CONTRACT,US_16_02_BRIDGE_REPAIR_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Wooden Planks,990,8600,US_16_02_BRIDGE_REPAIR_DESC,CONTRACTS\nUS_16_02_RAILWAY_ACCIDENTS_CONTRACT,US_16_02_RAILWAY_ACCIDENTS_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Railway; 1\u00d7 Railway,960,7150,US_16_02_RAILWAY_ACCIDENTS_DESC,CONTRACTS\nUS_16_02_ENERGY_DELIVERY_CONTRACT,US_16_02_ENERGY_DELIVERY_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Transformer,690,6100,US_16_02_ENERGY_DELIVERY_DESC,CONTRACTS\nUS_16_02_WILDERNESS_CONTRACT,US_16_02_WILDERNESS_CONTRACT,_CONTRACTS,US_16,Washington,exploration,,680,11850,US_16_02_WILDERNESS_DESC,CONTRACTS\nUS_16_02_CROOKED_PIT_TASK,US_16_02_CROOKED_PIT_TASK,_TASKS,US_16,Washington,truckDelivery,2\u00d7 Wooden Planks Medium,700,7800,US_16_02_CROOKED_PIT_DESC,TASKS\nUS_16_02_FUEL_TANK_TASK,US_16_02_FUEL_TANK_TASK,_TASKS,US_16,Washington,truckDelivery,,550,6200,US_16_02_FUEL_TANK_DESC,TASKS\nUS_16_02_WATER_MILL_TASK,US_16_02_WATER_MILL_TASK,_TASKS,US_16,Washington,truckDelivery,,710,6140,US_16_02_WATER_MILL_DESC,TASKS\nUS_16_02_BAD_CURIOSITY_TASK,US_16_02_BAD_CURIOSITY_TASK,_TASKS,US_16,Washington,truckDelivery,,460,4950,US_16_02_BAD_CURIOSITY_DESC,TASKS\nUS_16_02_SHARP_TURN_TASK,US_16_02_SHARP_TURN_TASK,_TASKS,US_16,Washington,truckDelivery,,360,3300,US_16_02_SHARP_TURN_DESC,TASKS\nUS_16_02_POTEST_TAMEN_TASK,US_16_02_POTEST_TAMEN_TASK,_TASKS,US_16,Washington,truckDelivery,,330,2700,US_16_02_POTEST_TAMEN_DESC,TASKS\nUS_16_02_BEAUTIFUL_STONES_TASK,US_16_02_BEAUTIFUL_STONES_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Stone Block,1110,8600,US_16_02_BEAUTIFUL_STONES_DESC,TASKS\nUS_16_02_FRIENDLY_HELP_TASK,US_16_02_FRIENDLY_HELP_TASK,_TASKS,US_16,Washington,cargoDelivery,2\u00d7 Potato; 1\u00d7 Soyabean,790,7700,US_16_02_FRIENDLY_HELP_DESC,TASKS\nUS_16_02_SCOUT_WATER_TASK,US_16_02_SCOUT_WATER_TASK,_TASKS,US_16,Washington,cargoDelivery,2200\u00d7 Water,820,7100,US_16_02_SCOUT_WATER_DESC,TASKS\nUS_16_02_SCOOTS_PAPER_TASK,US_16_02_SCOOTS_PAPER_TASK,_TASKS,US_16,Washington,cargoDelivery,4\u00d7 Cellulose,480,5550,US_16_02_SCOOTS_PAPER_DESC,TASKS\nUS_16_02_FERTILIZERS_SUPPLY_TASK,US_16_02_FERTILIZERS_SUPPLY_TASK,_TASKS,US_16,Washington,cargoDelivery,4\u00d7 Fertilizer,450,5100,US_16_02_FERTILIZERS_SUPPLY_DESC,TASKS\nUS_16_02_ROCK_BLOKER_TASK,US_16_02_ROCK_BLOKER_TASK,_TASKS,US_16,Washington,cargoDelivery,1\u00d7 Wooden Planks Medium,260,2500,US_16_02_ROCK_BLOKER_DESC,TASKS\nUS_16_02_RIVER_DRIVE_CONTEST,US_16_02_RIVER_DRIVE_CONTEST,_CONTESTS,US_16,Washington,exploration,,480,3900,US_16_02_RIVER_DRIVE_DESC,CONTESTS\nUS_16_02_TEST_RUNS_CONTEST,US_16_02_TEST_RUNS_CONTEST,_CONTESTS,US_16,Washington,exploration,,180,2200,US_16_02_TEST_RUNS_DESC,CONTESTS\nUS_16_03_DAM_RESTORATION_01_CONTRACT,US_16_03_DAM_RESTORATION_01_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Forklift Caravan Container 2,3320,38500,US_16_03_DAM_RESTORATION_01_DESC,CONTRACTS\nUS_16_03_HELPING_SCIENTISTS_01_CONTRACT,US_16_03_HELPING_SCIENTISTS_01_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Wooden Planks Medium; 1\u00d7 Solar Panel; 2\u00d7 Bricks; 2\u00d7 Bags,1380,14250,US_16_03_HELPING_SCIENTISTS_01_DESC,CONTRACTS\nUS_16_03_TRAILER_HELP_01_TASK,US_16_03_TRAILER_HELP_01_TASK,_TASKS,US_16,Washington,truckDelivery,,670,7850,US_16_03_TRAILER_HELP_01_DESC,TASKS\nUS_16_03_TRUCK_HELP_01_TASK,US_16_03_TRUCK_HELP_01_TASK,_TASKS,US_16,Washington,truckDelivery,,410,3450,US_16_03_TRUCK_HELP_01_DESC,TASKS\nUS_16_03_DAM_RESTORATION_02_CONTRACT,US_16_03_DAM_RESTORATION_02_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,4\u00d7 Bags; 3\u00d7 Metal Planks; 3\u00d7 Concrete Slab,3360,28550,US_16_03_DAM_RESTORATION_02_DESC,CONTRACTS\nUS_16_03_HELPING_SCIENTISTS_02_CONTRACT,US_16_03_HELPING_SCIENTISTS_02_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Bags 2; 1\u00d7 Solar Panel; 2\u00d7 Wooden Planks; 1\u00d7 Metal Planks,1410,13850,US_16_03_HELPING_SCIENTISTS_02_DESC,CONTRACTS\nUS_16_03_TRAILER_HELP_02_TASK,US_16_03_TRAILER_HELP_02_TASK,_TASKS,US_16,Washington,truckDelivery,4\u00d7 Service Spare Parts Special,1770,17600,US_16_03_TRAILER_HELP_02_DESC,TASKS\nUS_16_03_TRUCK_HELP_02_TASK,US_16_03_TRUCK_HELP_02_TASK,_TASKS,US_16,Washington,truckDelivery,,670,7952,US_16_03_TRUCK_HELP_02_DESC,TASKS\nUS_16_03_DAM_RESTORATION_03_CONTRACT,US_16_03_DAM_RESTORATION_03_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,2\u00d7 Bags; 2\u00d7 Bags 2; 4\u00d7 Bricks,2450,30700,US_16_03_DAM_RESTORATION_03_DESC,CONTRACTS\nUS_16_03_DAM_RESTORATION_04_CONTRACT,US_16_03_DAM_RESTORATION_04_CONTRACT,_CONTRACTS,US_16,Washington,cargoDelivery,1\u00d7 Transformer; 1\u00d7 Transformer,2200,27950,US_16_03_DAM_RESTORATION_04_DESC,CONTRACTS\nUS_16_03_VIEWPOINT_RESTORATION_CONTRACT,US_16_03_VIEWPOINT_RESTORATION_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,1\u00d7 Logs Long; 1\u00d7 Logs Medium; 2\u00d7 Wooden Planks Medium,2790,32450,US_16_03_VIEWPOINT_RESTORATION_DESC,CONTRACTS\nUS_16_03_RESTORATION_LOGGING_BASE_CONTRACT,US_16_03_RESTORATION_LOGGING_BASE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Service Spare Parts; 3\u00d7 Vehicles Spare Parts; 2\u00d7 Wooden Planks Medium,2240,28250,US_16_03_RESTORATION_LOGGING_BASE_DESC,CONTRACTS\nUS_16_03_RESTORATION_POWER_LINES_CONTRACT,US_16_03_RESTORATION_POWER_LINES_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Junk Open; 2\u00d7 Junk Closed; 2\u00d7 Metal Planks; 2\u00d7 Vehicles Spare Parts; 2\u00d7 Service Spare Parts,2980,26850,US_16_03_RESTORATION_POWER_LINES_DESC,CONTRACTS\nUS_16_03_HELIPAD_FUEL_CONTRACT,US_16_03_HELIPAD_FUEL_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,4\u00d7 Barrels,1520,19150,US_16_03_HELICOPTER_FUEL_DESC,CONTRACTS\nUS_16_03_BACK_TO_PLACE_CONTRACT,US_16_03_BACK_TO_PLACE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,4\u00d7 Vehicles Spare Parts,1780,18950,US_16_03_BACK_TO_PLACE_DESC,CONTRACTS\nUS_16_03_RESTORATING_SLAB_FACTORY_CONTRACT,US_16_03_RESTORATING_SLAB_FACTORY_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,3\u00d7 Metal Roll; 2\u00d7 Service Spare Parts,1330,17650,US_16_03_RESTORATING_SLAB_FACTORY_DESC,CONTRACTS\nUS_16_03_BRIDGE_BLOCKAGE_CONTRACT,US_16_03_BRIDGE_BLOCKAGE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Metal Planks; 2\u00d7 Blocks; 2\u00d7 Service Spare Parts,1750,16600,US_16_03_BRIDGE_BLOCKAGE_DESC,CONTRACTS\nUS_16_03_NEW_TRUCK_CONTRACT,US_16_03_NEW_TRUCK_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,,1190,14300,US_16_03_NEW_TRUCK_DESC,CONTRACTS\nUS_16_03_ROCK_BLOCKAGE_CONTRACT,US_16_03_ROCK_BLOCKAGE_CONTRACT,_CONTRACTS,US_16,Washington,truckDelivery,2\u00d7 Metal Planks; 2\u00d7 Wooden Planks Medium,1280,11600,US_16_03_ROCK_BLOCKAGE_DESC,CONTRACTS\nUS_16_03_MAIN_POINTS_CONTRACT,US_16_03_MAIN_POINTS_CONTRACT,_CONTRACTS,US_16,Washington,exploration,,350,5800,US_16_03_MAIN_POINTS_DESC,CONTRACTS\nUS_16_03_CONCRETE_DELIVERY_TASK,US_16_03_CONCRETE_DELIVERY_TASK,_TASKS,US_16,Washington,truckDelivery,1\u00d7 Blocks; 1\u00d7 Blocks; 1\u00d7 Concrete Slab,1640,16000,US_16_03_CONCRETE_DELIVERY_DESC,TASKS\nUS_16_03_TUBES_FOR_WELL_TASK,US_16_03_TUBES_FOR_WELL_TASK,_TASKS,US_16,Washington,truckDelivery,1\u00d7 Pipes Small,820,8750,US_16_03_TUBES_FOR_WELL_DESC,TASKS\nUS_16_03_TREE_BLOCKAGE_TASK,US_16_03_TREE_BLOCKAGE_TASK,_TASKS,US_16,Washington,truckDelivery,,570,6900,US_16_03_TREE_BLOCKAGE_DESC,TASKS\nUS_16_03_FISHING_FARM_TASK,US_16_03_FISHING_FARM_TASK,_TASKS,US_16,Washington,truckDelivery,2\u00d7 Barrels Chemicals,150,2500,US_16_03_FISHING_FARM_DESC,TASKS\nUS_16_03_WATER_PUMP_TASK,US_16_03_WATER_PUMP_TASK,_TASKS,US_16,Washington,cargoDelivery,1\u00d7 Npp Pump; 1\u00d7 Npp Pump,1210,11150,US_16_03_WATER_PUMP_DESC,TASKS\nUS_16_03_LOGS_FOR_HOUSE_TASK,US_16_03_LOGS_FOR_HOUSE_TASK,_TASKS,US_16,Washington,cargoDelivery,1\u00d7 Logs Medium; 1\u00d7 Logs Short,790,9550,US_16_03_LOGS_FOR_HOUSE_DESC,TASKS\nUS_16_03_AIRDROP_TASK,US_16_03_AIRDROP_TASK,_TASKS,US_16,Washington,cargoDelivery,3\u00d7 Cellulose,600,4250,US_16_03_AIRDROP_DESC,TASKS\nUS_16_03_WEATHER_TOWER_SCOUTING_TASK,US_16_03_WEATHER_TOWER_SCOUTING_TASK,_TASKS,US_16,Washington,exploration,,260,4250,US_16_03_WEATHER_TOWER_SCOUTING_DESC,TASKS\nUS_16_03_SHORE_SOIL_TASK,US_16_03_SHORE_SOIL_TASK,_TASKS,US_16,Washington,exploration,,230,3850,US_16_03_SHORE_SOIL_DESC,TASKS\nUS_16_03_LOGGONG_BASE_CONTEST,US_16_03_LOGGONG_BASE_CONTEST,_CONTESTS,US_16,Washington,exploration,,120,1900,US_16_03_LOGGING_BASE_DESC,CONTESTS\nUS_16_03_SCOUT_CAMP_CONTEST,US_16_03_SCOUT_CAMP_CONTEST,_CONTESTS,US_16,Washington,exploration,,90,1700,US_16_03_SCOUT_CAMP_DESC,CONTESTS\nRU_17_01_DRILL_WATER_01_CONTRACT,RU_17_01_DRILL_WATER_01_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,1\u00d7 Vehicles Spare Parts,810,8400,RU_17_01_DRILL_WATER_01_DESC,CONTRACTS\nRU_17_01_TRAILER_DELIVERY_01_TASK,RU_17_01_TRAILER_DELIVERY_01_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,620,7000,RU_17_01_TRAILER_DELIVERY_01_DESC,TASKS\nRU_17_01_TRUCK_DELIVERY_01_TASK,RU_17_01_TRUCK_DELIVERY_01_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,610,6800,RU_17_01_TRUCK_DELIVERY_01_DESC,TASKS\nRU_17_01_SCOUT_DELIVERY_01_TASK,RU_17_01_SCOUT_DELIVERY_01_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,530,6150,RU_17_01_SCOUT_DELIVERY_01_DESC,TASKS\nRU_17_01_SCOUT_TRAILER_01_TASK,RU_17_01_SCOUT_TRAILER_01_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,550,5800,RU_17_01_SCOUT_TRAILER_01_DESC,TASKS\nRU_17_01_DRILL_WATER_02_CONTRACT,RU_17_01_DRILL_WATER_02_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,1\u00d7 Service Spare Parts,1190,13900,RU_17_01_DRILL_WATER_02_DESC,CONTRACTS\nRU_17_01_TRUCK_DELIVERY_02_TASK,RU_17_01_TRUCK_DELIVERY_02_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,590,6500,RU_17_01_TRUCK_DELIVERY_02_DESC,TASKS\nRU_17_01_SCOUT_DELIVERY_02_TASK,RU_17_01_SCOUT_DELIVERY_02_TASK,_TASKS,RU_17,Zurdania,truckDelivery,,420,3500,RU_17_01_SCOUT_DELIVERY_02_DESC,TASKS\nRU_17_01_FIELD_CLEAR_CONTRACT,RU_17_01_FIELD_CLEAR_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Crate Large; 4\u00d7 Fertilizer,3070,33300,RU_17_01_FIELD_CLEAR_DESC,CONTRACTS\nRU_17_01_TUNNEL_CONTRACT,RU_17_01_TUNNEL_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,4\u00d7 Blocks; 3\u00d7 Bags,2510,26800,RU_17_01_TUNNEL_DESC,CONTRACTS\nRU_17_01_FIELDS_FERTILIZER_CONTRACT,RU_17_01_FIELDS_FERTILIZER_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,6\u00d7 Fertilizer,1850,16450,RU_17_01_FIELDS_FERTILIZER_DESC,CONTRACTS\nRU_17_01_FARM_SOLAR_CONTRACT,RU_17_01_FARM_SOLAR_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Wooden Planks; 2\u00d7 Metal Planks; 4\u00d7 Fertilizer; 1\u00d7 Solar Panel; 1\u00d7 Solar Panel; 1\u00d7 Solar Panel,3620,36600,RU_17_01_FARM_SOLAR_CONTRACT_DESC,CONTRACTS\nRU_17_01_DELIVERY_FOODS_CONTRACT,RU_17_01_DELIVERY_FOODS_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Container Reefer; 1\u00d7 Container Small; 1\u00d7 Container Large; 1\u00d7 Container Reefer,2880,30050,RU_17_01_DELIVERY_FOODS_DESC,CONTRACTS\nRU_17_01_FIELD_WORK_CONTRACT,RU_17_01_FIELD_WORK_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,4\u00d7 Fertilizer; 2\u00d7 Crate Large; 3\u00d7 Service Spare Parts; 1\u00d7 Pipes Medium,2880,28700,RU_17_01_FIELD_WORK_DESC,CONTRACTS\nRU_17_01_FACTORY_BUILD_CONTRACT,RU_17_01_FACTORY_BUILD_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Metal Roll; 6\u00d7 Bricks; 1\u00d7 Chiller,2960,28600,RU_17_01_FACTORY_BUILD_DESC,CONTRACTS\nRU_17_01_OIL_COMPANY_CONTRACT,RU_17_01_OIL_COMPANY_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,4\u00d7 Barrels; 2\u00d7 Bricks; 2\u00d7 Bags 2,2390,28200,RU_17_01_OIL_COMPANY_DESC,CONTRACTS\nRU_17_01_WATER_TOWERS_CONTRACT,RU_17_01_WATER_TOWERS_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Metal Planks; 2\u00d7 Metal Roll; 2\u00d7 Metal Planks; 2\u00d7 Metal Roll,2860,26950,RU_17_01_WATER_TOWERS_DESC,CONTRACTS\nRU_17_01_DELIVERY_LOGS_CONTRACT,RU_17_01_DELIVERY_LOGS_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Logs Short; 1\u00d7 Logs Medium; 1\u00d7 Logs Short; 1\u00d7 Logs Medium,1920,23700,RU_17_01_DELIVERY_LOGS_DESC,CONTRACTS\nRU_17_01_OPEN_GATE_CONTRACT,RU_17_01_OPEN_GATE_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Blocks; 2\u00d7 Bags; 2\u00d7 Bags 2,1870,20450,RU_17_01_OPEN_GATE_DESC,CONTRACTS\nRU_17_01_BUILD_BRIDGE_CONTRACT,RU_17_01_BUILD_BRIDGE_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Container Large; 2\u00d7 Concrete Slab; 1\u00d7 Metal Planks,2260,19200,RU_17_01_BUILD_BRIDGE_DESC,CONTRACTS\nRU_17_01_ROAD_TROUBLE_CONTRACT,RU_17_01_ROAD_TROUBLE_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Junk Open; 2\u00d7 Crate Large; 2\u00d7 Junk Closed,1990,17900,RU_17_01_ROAD_TROUBLE_DESC,CONTRACTS\nRU_17_01_HOLLYWOOD_CONTRACT,RU_17_01_HOLLYWOOD_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Crate Large; 1\u00d7 Service Spare Parts; 2\u00d7 Wooden Planks,1470,16900,RU_17_01_HOLLYWOOD_DESC,CONTRACTS\nRU_17_01_FOOD_WAREHOUSE_CONTRACT,RU_17_01_FOOD_WAREHOUSE_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Wooden Planks; 4\u00d7 Metal Roll; 2\u00d7 Container Reefer,1720,15700,RU_17_01_FOOD_WAREHOUSE_DESC,CONTRACTS\nRU_17_01_CONTAINER_DELIVERY_CONTRACT,RU_17_01_CONTAINER_DELIVERY_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Container Large; 2\u00d7 Container Small,1420,12350,RU_17_01_CONTAINER_DELIVERY_DESC,CONTRACTS\nRU_17_01_DELIVERY_WAREHOUSE_CONTRACT,RU_17_01_DELIVERY_WAREHOUSE_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,3\u00d7 Logs Short; 4\u00d7 Bags 2,1320,11900,RU_17_01_DELIVERY_WAREHOUSE_DESC,CONTRACTS\nRU_17_01_SCOUTING_READY_CONTRACT,RU_17_01_SCOUTING_READY_CONTRACT,_CONTRACTS,RU_17,Zurdania,exploration,,790,13750,RU_17_01_SCOUTING_READY_DESC,CONTRACTS\nRU_17_01_SEISMO_CHECK_CONTRACT,RU_17_01_SEISMO_CHECK_CONTRACT,_CONTRACTS,RU_17,Zurdania,exploration,,780,13550,RU_17_01_SEISMO_CHECK_DESC,CONTRACTS\nRU_17_01_ROCK_TOWNS_TASK,RU_17_01_ROCK_TOWNS_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Stack; 1\u00d7 Wooden Planks; 1\u00d7 Crate Large,1450,17100,RU_17_01_ROCK_TOWNS_DESC,TASKS\nRU_17_01_WATCHTOWERS_HELP_TSK,RU_17_01_WATCHTOWERS_HELP_TSK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Crate Large; 1\u00d7 Barrels; 1\u00d7 Crate Large,1300,14650,RU_17_01_WATCHTOWERS_HELP_DESC,TASKS\nRU_17_01_MAGAZINES_DELIVERY_TASK,RU_17_01_MAGAZINES_DELIVERY_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Barrels; 800\u00d7 Water; 1\u00d7 Crate Large,1320,14050,RU_17_01_MAGAZINES_DELIVERY_DESC,TASKS\nRU_17_01_RAILWAY_DELIVERY_TASK,RU_17_01_RAILWAY_DELIVERY_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Pipes Small,1280,13400,RU_17_01_RAILWAY_DELIVERY_DESC,TASKS\nRU_17_01_METAL_CITY_TASK,RU_17_01_METAL_CITY_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Metal Roll; 1\u00d7 Metal Planks,1160,12250,RU_17_01_METAL_CITY_DESC,TASKS\nRU_17_01_HOUSE_ON_THE_ROCK_TASK,RU_17_01_HOUSE_ON_THE_ROCK_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Service Spare Parts; 3\u00d7 Solar Panel,1170,11450,RU_17_01_HOUSE_ON_THE_ROCK_DESC,TASKS\nRU_17_01_CITY_VIEW_TASK,RU_17_01_CITY_VIEW_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Service Spare Parts; 1\u00d7 Service Spare Parts; 1\u00d7 Crate Large,960,10750,RU_17_01_CITY_VIEW_DESC,TASKS\nRU_17_01_FOREST_HOUSE_TASK,RU_17_01_FOREST_HOUSE_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Stack; 1\u00d7 Stack; 2\u00d7 Barrels,1020,10150,RU_17_01_FOREST_HOUSE_DESC,TASKS\nRU_17_01_GROUP_HOUSE_TASK,RU_17_01_GROUP_HOUSE_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Solar Panel; 1\u00d7 Service Spare Parts,780,8600,RU_17_01_GROUP_HOUSE_DESC,TASKS\nRU_17_01_OIL_DELIVERY_TASK,RU_17_01_OIL_DELIVERY_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Container Small; 1\u00d7 Container Large,1100,8350,RU_17_01_OIL_DELIVERY_DESC,TASKS\nRU_17_01_CROSS_HOUSE_TASK,RU_17_01_CROSS_HOUSE_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Barrels; 1\u00d7 Container Reefer,770,7350,RU_17_01_CROSS_HOUSE_DESC,TASKS\nRU_17_01_FARM_HELP_TASK,RU_17_01_FARM_HELP_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Rubbish,750,7100,RU_17_01_FARM_HELP_DESC,TASKS\nRU_17_01_HOUSE_ALONE_TASK,RU_17_01_HOUSE_ALONE_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Bags 2; 1\u00d7 Wooden Planks,720,6800,RU_17_01_HOUSE_ALONE_DESC,TASKS\nRU_17_01_RAILWAY_CHECK_CONTEST,RU_17_01_RAILWAY_CHECK_CONTEST,_CONTESTS,RU_17,Zurdania,exploration,,30,500,RU_17_01_RAILWAY_CHECK_DESC,CONTESTS\nRU_17_01_CLIMB_PLATFORM_CONTEST,RU_17_01_CLIMB_PLATFORM_CONTEST,_CONTESTS,RU_17,Zurdania,exploration,,20,300,RU_17_01_CLIMB_PLATFORM_DESC,CONTESTS\nRU_17_02_TRAIN_01_CONTRACT,RU_17_02_TRAIN_01_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,,330,2350,RU_17_02_TRAIN_01_DESC,CONTRACTS\nRU_17_02_SOLAR_ENERGY_CONTRACT_01,RU_17_02_SOLAR_ENERGY_CONTRACT_01,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Metal Planks; 1\u00d7 Bags; 1\u00d7 Metal Planks; 1\u00d7 Bags; 1\u00d7 Metal Planks; 1\u00d7 Bags; 1\u00d7 Metal Planks; 1\u00d7 Bags; 1\u00d7 Service Spare Parts,3110,32850,RU_17_02_SOLAR_ENERGY_01_DESC,CONTRACTS\nRU_17_02_VILLAGE_SUPPLY_TASK_01,RU_17_02_VILLAGE_SUPPLY_TASK_01,_TASKS,RU_17,Zurdania,truckDelivery,2\u00d7 Bags; 1\u00d7 Wooden Planks Medium,1350,16500,RU_17_02_VILLAGE_SUPPLY_01_DESC,TASKS\nRU_17_02_TRUCK_HELP_01,RU_17_02_TRUCK_HELP_01,_TASKS,RU_17,Zurdania,truckDelivery,,700,9200,RU_17_02_TRUCK_HELP_01_DESC,TASKS\nRU_17_02_SOLAR_ENERGY_CONTRACT_02,RU_17_02_SOLAR_ENERGY_CONTRACT_02,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Solar Panel; 2\u00d7 Solar Panel; 2\u00d7 Solar Panel; 2\u00d7 Solar Panel,1890,19900,RU_17_02_SOLAR_ENERGY_02_DESC,CONTRACTS\nRU_17_02_TRAILER_HELP_02,RU_17_02_TRAILER_HELP_02,_TASKS,RU_17,Zurdania,truckDelivery,3\u00d7 Service Spare Parts Special,1390,15200,RU_17_02_TRAILER_HELP_02_DESC,TASKS\nRU_17_02_TRUCK_HELP_02,RU_17_02_TRUCK_HELP_02,_TASKS,RU_17,Zurdania,truckDelivery,,470,4700,RU_17_02_TRUCK_HELP_02_DESC,TASKS\nRU_17_02_VILLAGE_SUPPLY_TASK_02,RU_17_02_VILLAGE_SUPPLY_TASK_02,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Bags; 1\u00d7 Logs Short,980,11950,RU_17_02_VILLAGE_SUPPLY_02_DESC,TASKS\nRU_17_02_SOLAR_ENERGY_CONTRACT_03,RU_17_02_SOLAR_ENERGY_CONTRACT_03,_CONTRACTS,RU_17,Zurdania,truckDelivery,1\u00d7 Transformer,1190,12600,RU_17_02_SOLAR_ENERGY_03_DESC,CONTRACTS\nRU_17_02_TRUCK_HELP_03,RU_17_02_TRUCK_HELP_03,_TASKS,RU_17,Zurdania,truckDelivery,,380,4000,RU_17_02_TRUCK_HELP_03_DESC,TASKS\nRU_17_02_VILLAGE_SUPPLY_TASK_03,RU_17_02_VILLAGE_SUPPLY_TASK_03,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Bricks; 1\u00d7 Wooden Planks Medium; 1\u00d7 Wooden Planks,1300,16650,RU_17_02_VILLAGE_SUPPLY_03_DESC,TASKS\nRU_17_02_TRUCK_HELP_04,RU_17_02_TRUCK_HELP_04,_TASKS,RU_17,Zurdania,truckDelivery,,710,7650,RU_17_02_TRUCK_HELP_04_DESC,TASKS\nRU_17_02_RAILWAY_ACCIDENT_CONTRACT,RU_17_02_RAILWAY_ACCIDENT_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,400\u00d7 Consumable Repairs; 4\u00d7 Junk Open; 4\u00d7 Junk Closed,4050,40300,RU_17_02_RAILWAY_ACCIDENT_DESC,CONTRACTS\nRU_17_02_SANATORIUM_RESTORATION_CONTRACT,RU_17_02_SANATORIUM_RESTORATION_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,1\u00d7 Metal Planks; 2\u00d7 Bricks; 1\u00d7 Bags; 2\u00d7 Wooden Planks; 1\u00d7 Wooden Planks Medium,3110,37500,RU_17_02_SANATORIUM_RESTORATION_DESC,CONTRACTS\nRU_17_02_MONASTERY_RESTORATION_CONTRACT,RU_17_02_MONASTERY_RESTORATION_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,2\u00d7 Bags 2; 2\u00d7 Bags; 2\u00d7 Bricks,2860,35250,RU_17_02_MONASTERY_RESTORATION_DESC,CONTRACTS\nRU_17_02_OLD_FACTORY_CONTRACT,RU_17_02_OLD_FACTORY_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,1\u00d7 Metal Planks; 1\u00d7 Pipes Medium; 1\u00d7 Service Spare Parts,1850,20900,RU_17_02_OLD_FACTORY_DESC,CONTRACTS\nRU_17_02_ASPHALT_BLOCKAGE_CONTRACT,RU_17_02_ASPHALT_BLOCKAGE_CONTRACT,_CONTRACTS,RU_17,Zurdania,truckDelivery,200\u00d7 Consumable Repairs; 2\u00d7 Junk Open; 2\u00d7 Junk Closed,2120,18350,RU_17_02_ASPHALT_BLOCKAGE_DESC,CONTRACTS\nRU_17_02_SANATORIUM_WATER_SYSTEM_CONTRACT,RU_17_02_SANATORIUM_WATER_SYSTEM_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Pipes Small; 1\u00d7 Pipes Medium; 1\u00d7 Boiler,3090,36850,RU_17_02_SANATORIUM_WATER_SYSTEM_DESC,CONTRACTS\nRU_17_02_FERTILIZERS_CONTRACT,RU_17_02_FERTILIZERS_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Fertilizer; 1\u00d7 Fertilizer; 1\u00d7 Fertilizer; 2\u00d7 Fertilizer,2020,22650,RU_17_02_FERTILIZERS_DESC,CONTRACTS\nRU_17_02_PLATFORM_RESTORATION_CONTRACT,RU_17_02_PLATFORM_RESTORATION_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Concrete Slab; 1\u00d7 Metal Planks; 1\u00d7 Concrete Slab; 2\u00d7 Bags,1980,20900,RU_17_02_PLATFORM_RESTORATION_DESC,CONTRACTS\nRU_17_02_HOLLY_WATER_CONTRACT,RU_17_02_HOLLY_WATER_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,750\u00d7 Water; 350\u00d7 Water; 500\u00d7 Water,1740,17950,RU_17_02_HOLLY_WATER_DESC,CONTRACTS\nRU_17_02_HYDRO_RESEARCH_CONTRACT,RU_17_02_HYDRO_RESEARCH_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy; 1\u00d7 Sensory Buoy,1350,14950,RU_17_02_HYDRO_RESEARCH_DESC,CONTRACTS\nRU_17_02_VILLAGE_AID_CONTRACT,RU_17_02_VILLAGE_AID_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,3\u00d7 Barrels Oil; 2\u00d7 Wooden Planks; 2\u00d7 Bricks,1230,14150,RU_17_02_VILLAGE_AID_DESC,CONTRACTS\nRU_17_02_SANATORIUM_FOOD_CONTRACT,RU_17_02_SANATORIUM_FOOD_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Container Reefer; 2\u00d7 Service Spare Parts,1300,13150,RU_17_02_SANATORIUM_FOOD_DESC,CONTRACTS\nRU_17_02_CHAPEL_RESTORATION_CONTRACT,RU_17_02_CHAPEL_RESTORATION_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Bags; 1\u00d7 Wooden Planks; 1\u00d7 Bags 2,980,10400,RU_17_02_CHAPEL_RESTORATION_DESC,CONTRACTS\nRU_17_02_SMALL_ACCIDENT_CONTRACT,RU_17_02_SMALL_ACCIDENT_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,2\u00d7 Wooden Planks; 1\u00d7 Wooden Planks Medium,640,7500,RU_17_02_SMALL_ACCIDENT_DESC,CONTRACTS\nRU_17_02_RAILWAY_RESTORATION_CONTRACT,RU_17_02_RAILWAY_RESTORATION_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,1\u00d7 Railway; 1\u00d7 Railway,780,6050,RU_17_02_RAILWAY_RESTORATION_DESC,CONTRACTS\nRU_17_02_FARM_WATER_CONTRACT,RU_17_02_FARM_WATER_CONTRACT,_CONTRACTS,RU_17,Zurdania,cargoDelivery,3200\u00d7 Water,400,5250,RU_17_02_FARM_WATER_DESC,CONTRACTS\nRU_17_02_SEISMIC_HILLS_CONTRACT,RU_17_02_SEISMIC_HILLS_CONTRACT,_CONTRACTS,RU_17,Zurdania,exploration,,640,11050,RU_17_02_SEISMIC_HILLS_DESC,CONTRACTS\nRU_17_02_FIRST_SCOUT_CONTRACT,RU_17_02_FIRST_SCOUT_CONTRACT,_CONTRACTS,RU_17,Zurdania,exploration,,340,5850,RU_17_02_FIRST_SCOUT_DESC,CONTRACTS\nRU_17_02_SCANER_TASK,RU_17_02_SCANER_TASK,_TASKS,RU_17,Zurdania,truckDelivery,3\u00d7 Barrels Oil,710,7650,RU_17_02_SCANER_DESC,TASKS\nRU_17_02_LUMBER_SUPPLY_TASK,RU_17_02_LUMBER_SUPPLY_TASK,_TASKS,RU_17,Zurdania,truckDelivery,2\u00d7 Service Spare Parts; 2\u00d7 Barrels Oil,400,4750,RU_17_02_LUMBER_SUPPLY_DESC,TASKS\nRU_17_02_SWAMP_WOOD_TASK,RU_17_02_SWAMP_WOOD_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,2\u00d7 Logs Short; 1\u00d7 Logs Medium,780,8800,RU_17_02_SWAMP_WOOD_DESC,TASKS\nRU_17_02_VILLAGE_WOOD_TASK,RU_17_02_VILLAGE_WOOD_TASK,_TASKS,RU_17,Zurdania,cargoDelivery,1\u00d7 Logs Long,600,6300,RU_17_02_VILLAGE_WOOD_DESC,TASKS\nRU_17_02_SEISMIC_SWAMP_TASK,RU_17_02_SEISMIC_SWAMP_TASK,_TASKS,RU_17,Zurdania,exploration,,130,2200,RU_17_02_SEISMIC_SWAMP_DESC,TASKS\nRU_17_02_MOUNTAIN_SCOUT_TASK,RU_17_02_MOUNTAIN_SCOUT_TASK,_TASKS,RU_17,Zurdania,exploration,,120,2000,RU_17_02_MOUNTAIN_SCOUT_DESC,TASKS\nRU_17_02_NORTH_SCOUT_TASK,RU_17_02_NORTH_SCOUT_TASK,_TASKS,RU_17,Zurdania,exploration,,120,1950,RU_17_02_NORTH_SCOUT_DESC,TASKS\nRU_17_02_SWAMP_RACING,RU_17_02_SWAMP_RACING,_CONTESTS,RU_17,Zurdania,exploration,,150,1600,RU_17_02_SWAMP_RACING_DESC,CONTESTS\nRU_17_02_MOUNTAIN_RACING,RU_17_02_MOUNTAIN_RACING,_CONTESTS,RU_17,Zurdania,exploration,,150,1550,RU_17_02_MOUNTAIN_RACING_DESC,CONTESTS\n";

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
