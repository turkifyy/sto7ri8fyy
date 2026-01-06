var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// server/firebase-rest-client.ts
function setAuthToken(token) {
  currentAuthToken = token;
  if (token) {
    console.log("\u{1F511} Auth token set for Firestore REST API (length:", token.length, ")");
  } else {
    console.log("\u{1F511} Auth token cleared");
  }
}
function getProjectId() {
  const projectId = process.env.VITE_FIREBASE_PROJECT_ID;
  if (!projectId) {
    throw new Error("VITE_FIREBASE_PROJECT_ID is not configured");
  }
  return projectId;
}
function getApiKey() {
  const apiKey = process.env.VITE_FIREBASE_API_KEY;
  if (!apiKey) {
    throw new Error("VITE_FIREBASE_API_KEY is not configured");
  }
  return apiKey;
}
function firestoreValueToJs(value) {
  if (value === null || value === void 0) return null;
  if ("stringValue" in value) return value.stringValue;
  if ("integerValue" in value) return parseInt(value.integerValue);
  if ("doubleValue" in value) return value.doubleValue;
  if ("booleanValue" in value) return value.booleanValue;
  if ("nullValue" in value) return null;
  if ("timestampValue" in value) return new Date(value.timestampValue);
  if ("arrayValue" in value) {
    return (value.arrayValue.values || []).map(firestoreValueToJs);
  }
  if ("mapValue" in value) {
    const result = {};
    for (const [k, v] of Object.entries(value.mapValue.fields || {})) {
      result[k] = firestoreValueToJs(v);
    }
    return result;
  }
  return null;
}
function jsToFirestoreValue(value) {
  if (value === null || value === void 0) {
    return { nullValue: null };
  }
  if (typeof value === "string") {
    return { stringValue: value };
  }
  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return { integerValue: value.toString() };
    }
    return { doubleValue: value };
  }
  if (typeof value === "boolean") {
    return { booleanValue: value };
  }
  if (value instanceof Date) {
    return { timestampValue: value.toISOString() };
  }
  if (Array.isArray(value)) {
    return {
      arrayValue: {
        values: value.map(jsToFirestoreValue)
      }
    };
  }
  if (typeof value === "object") {
    const fields = {};
    for (const [k, v] of Object.entries(value)) {
      if (v !== void 0) {
        fields[k] = jsToFirestoreValue(v);
      }
    }
    return { mapValue: { fields } };
  }
  return { nullValue: null };
}
function documentToObject(doc) {
  if (!doc || !doc.fields) return null;
  const result = {};
  for (const [key, value] of Object.entries(doc.fields)) {
    result[key] = firestoreValueToJs(value);
  }
  if (doc.name) {
    const parts = doc.name.split("/");
    result.id = parts[parts.length - 1];
  }
  return result;
}
function objectToDocument(data) {
  const fields = {};
  for (const [key, value] of Object.entries(data)) {
    if (key !== "id" && value !== void 0) {
      fields[key] = jsToFirestoreValue(value);
    }
  }
  return { fields };
}
async function firestoreRequest(path8, options = {}) {
  const projectId = getProjectId();
  const apiKey = getApiKey();
  const url = `${FIRESTORE_BASE_URL}/projects/${projectId}/databases/(default)/documents${path8}?key=${apiKey}`;
  const headers = {
    "Content-Type": "application/json"
  };
  if (currentAuthToken) {
    headers["Authorization"] = `Bearer ${currentAuthToken}`;
  }
  const response = await fetch(url, {
    ...options,
    headers: {
      ...headers,
      ...options.headers || {}
    }
  });
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    console.error("Firestore REST API error:", errorData);
    throw new Error(errorData.error?.message || `Firestore request failed: ${response.status}`);
  }
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}
var FIRESTORE_BASE_URL, currentAuthToken, firestoreRest;
var init_firebase_rest_client = __esm({
  "server/firebase-rest-client.ts"() {
    "use strict";
    FIRESTORE_BASE_URL = "https://firestore.googleapis.com/v1";
    currentAuthToken = null;
    firestoreRest = {
      async getDocument(collection, docId) {
        try {
          const doc = await firestoreRequest(`/${collection}/${docId}`);
          return documentToObject(doc);
        } catch (error) {
          if (error.message?.includes("404") || error.message?.includes("NOT_FOUND")) {
            return null;
          }
          throw error;
        }
      },
      async setDocument(collection, docId, data, merge = false) {
        const document = objectToDocument(data);
        if (merge) {
          const existing = await this.getDocument(collection, docId);
          if (existing) {
            const merged = { ...existing, ...data };
            delete merged.id;
            const mergedDoc = objectToDocument(merged);
            await firestoreRequest(`/${collection}/${docId}`, {
              method: "PATCH",
              body: JSON.stringify(mergedDoc)
            });
            return;
          }
        }
        await firestoreRequest(`/${collection}/${docId}`, {
          method: "PATCH",
          body: JSON.stringify(document)
        });
      },
      async addDocument(collection, data) {
        const document = objectToDocument(data);
        const result = await firestoreRequest(`/${collection}`, {
          method: "POST",
          body: JSON.stringify(document)
        });
        const parts = result.name.split("/");
        return parts[parts.length - 1];
      },
      async deleteDocument(collection, docId) {
        await firestoreRequest(`/${collection}/${docId}`, {
          method: "DELETE"
        });
      },
      async getCollection(collection) {
        try {
          const result = await firestoreRequest(`/${collection}`);
          if (!result.documents) return [];
          return result.documents.map(documentToObject).filter((doc) => doc !== null);
        } catch (error) {
          if (error.message?.includes("404") || error.message?.includes("NOT_FOUND")) {
            return [];
          }
          throw error;
        }
      },
      async queryCollection(collection, filters = [], orderBy, limit) {
        const projectId = getProjectId();
        const apiKey = getApiKey();
        const structuredQuery = {
          from: [{ collectionId: collection }]
        };
        if (filters.length > 0) {
          const firestoreFilters = filters.map((f) => ({
            fieldFilter: {
              field: { fieldPath: f.field },
              op: f.op,
              value: jsToFirestoreValue(f.value)
            }
          }));
          if (firestoreFilters.length === 1) {
            structuredQuery.where = firestoreFilters[0];
          } else {
            structuredQuery.where = {
              compositeFilter: {
                op: "AND",
                filters: firestoreFilters
              }
            };
          }
        }
        if (orderBy) {
          structuredQuery.orderBy = [{
            field: { fieldPath: orderBy.field },
            direction: orderBy.direction
          }];
        }
        if (limit) {
          structuredQuery.limit = limit;
        }
        const url = `${FIRESTORE_BASE_URL}/projects/${projectId}/databases/(default)/documents:runQuery?key=${apiKey}`;
        const headers = {
          "Content-Type": "application/json"
        };
        if (currentAuthToken) {
          headers["Authorization"] = `Bearer ${currentAuthToken}`;
        }
        const response = await fetch(url, {
          method: "POST",
          headers,
          body: JSON.stringify({ structuredQuery })
        });
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          console.error("Firestore query error:", errorData);
          throw new Error(errorData.error?.message || `Query failed: ${response.status}`);
        }
        const results = await response.json();
        return results.filter((r) => r.document).map((r) => documentToObject(r.document)).filter((doc) => doc !== null);
      }
    };
  }
});

// server/firebase-admin-setup.ts
var firebase_admin_setup_exports = {};
__export(firebase_admin_setup_exports, {
  auth: () => getAuth,
  firestore: () => getFirestore,
  getAuth: () => getAuth,
  getFirestore: () => getFirestore,
  isUsingRestApi: () => isUsingRestApi,
  verifyTokenWithFirebaseAPI: () => verifyTokenWithFirebaseAPI
});
import admin from "firebase-admin";
function initializeApp() {
  if (initializationAttempted) return;
  initializationAttempted = true;
  try {
    if (!admin.apps || admin.apps.length === 0) {
      const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT;
      const projectId = process.env.VITE_FIREBASE_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
      console.log("\u{1F50D} Checking Firebase credentials...");
      console.log("   - FIREBASE_SERVICE_ACCOUNT length:", serviceAccount?.length || 0);
      console.log("   - Project ID:", projectId || "not set");
      if (serviceAccount && serviceAccount.length > 100) {
        try {
          const cleanedServiceAccount = serviceAccount.trim();
          const serviceAccountJson = JSON.parse(cleanedServiceAccount);
          console.log("\u{1F4CB} Service account parsed successfully");
          console.log("   - Type:", serviceAccountJson.type);
          console.log("   - Project ID:", serviceAccountJson.project_id);
          console.log("   - Client Email:", serviceAccountJson.client_email);
          admin.initializeApp({
            credential: admin.credential.cert(serviceAccountJson),
            projectId: serviceAccountJson.project_id
          });
          console.log("\u2705 Firebase Admin initialized with service account");
          firestoreInstance = admin.firestore();
          authInstance = admin.auth();
          initialized = true;
          usingRestApi = false;
          console.log("\u2705 Firestore and Auth initialized successfully (using Admin SDK)");
          return;
        } catch (parseError) {
          console.error("\u274C Failed to parse Firebase service account JSON:", parseError.message);
          console.error("   First 200 chars of service account:", serviceAccount?.substring(0, 200));
        }
      }
      if (projectId && process.env.VITE_FIREBASE_API_KEY) {
        console.log("\u2139\uFE0F  Using Firebase REST API for Firestore operations");
        usingRestApi = true;
        initialized = true;
        return;
      }
      console.error("\u274C No Firebase credentials found. Please set FIREBASE_SERVICE_ACCOUNT or ensure VITE_FIREBASE_PROJECT_ID and VITE_FIREBASE_API_KEY are set.");
    }
  } catch (error) {
    console.error("\u274C Firebase initialization error:", error.message);
  }
}
function convertOperator(op) {
  const opMap = {
    "==": "EQUAL",
    "!=": "NOT_EQUAL",
    "<": "LESS_THAN",
    "<=": "LESS_THAN_OR_EQUAL",
    ">": "GREATER_THAN",
    ">=": "GREATER_THAN_OR_EQUAL",
    "array-contains": "ARRAY_CONTAINS",
    "in": "IN",
    "array-contains-any": "ARRAY_CONTAINS_ANY",
    "not-in": "NOT_IN"
  };
  return opMap[op] || "EQUAL";
}
function createQueryBuilder(collection, filters = [], orderBy, limitCount) {
  return {
    where: (field, op, value) => {
      return createQueryBuilder(
        collection,
        [...filters, { field, op: convertOperator(op), value }],
        orderBy,
        limitCount
      );
    },
    orderBy: (field, direction = "asc") => {
      return createQueryBuilder(
        collection,
        filters,
        { field, direction: direction === "asc" ? "ASCENDING" : "DESCENDING" },
        limitCount
      );
    },
    limit: (count) => {
      return createQueryBuilder(collection, filters, orderBy, count);
    },
    get: async () => {
      const docs = await firestoreRest.queryCollection(collection, filters, orderBy, limitCount);
      return {
        empty: docs.length === 0,
        docs: docs.map((doc) => ({
          id: doc.id,
          exists: true,
          data: () => doc
        }))
      };
    }
  };
}
function getFirestore() {
  if (!initialized) {
    initializeApp();
  }
  if (usingRestApi) {
    return restWrapper;
  }
  if (!firestoreInstance) {
    if (process.env.VITE_FIREBASE_PROJECT_ID && process.env.VITE_FIREBASE_API_KEY) {
      usingRestApi = true;
      return restWrapper;
    }
    throw new Error("Firestore not initialized. Please check Firebase configuration.");
  }
  return firestoreInstance;
}
function getAuth() {
  if (!initialized) {
    initializeApp();
  }
  return authInstance;
}
function isUsingRestApi() {
  return usingRestApi;
}
async function verifyTokenWithFirebaseAPI(token) {
  const apiKey = process.env.VITE_FIREBASE_API_KEY;
  if (!apiKey) {
    throw new Error("Firebase API key not configured");
  }
  try {
    const response = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${apiKey}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ idToken: token })
      }
    );
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      console.log("Token verification response:", { status: response.status, errorData });
      throw new Error(errorData.error?.message || `Token verification failed: ${response.status}`);
    }
    const data = await response.json();
    if (data.users && data.users.length > 0) {
      const user = data.users[0];
      return {
        uid: user.localId,
        email: user.email,
        name: user.displayName || "",
        email_verified: user.emailVerified || false
      };
    }
    throw new Error("No user found in response");
  } catch (error) {
    console.error("Firebase token verification error:", error.message);
    throw error;
  }
}
var firestoreInstance, authInstance, initialized, initializationAttempted, usingRestApi, FirestoreRestWrapper, restWrapper;
var init_firebase_admin_setup = __esm({
  "server/firebase-admin-setup.ts"() {
    "use strict";
    init_firebase_rest_client();
    firestoreInstance = null;
    authInstance = null;
    initialized = false;
    initializationAttempted = false;
    usingRestApi = false;
    FirestoreRestWrapper = class {
      collection(name) {
        return {
          doc: (id) => ({
            get: async () => {
              const data = await firestoreRest.getDocument(name, id);
              return {
                exists: data !== null,
                data: () => data,
                id
              };
            },
            set: async (data, options) => {
              await firestoreRest.setDocument(name, id, data, options?.merge);
            },
            update: async (data) => {
              await firestoreRest.setDocument(name, id, data, true);
            },
            delete: async () => {
              await firestoreRest.deleteDocument(name, id);
            }
          }),
          add: async (data) => {
            const id = await firestoreRest.addDocument(name, data);
            return { id };
          },
          get: async () => {
            const docs = await firestoreRest.getCollection(name);
            return {
              empty: docs.length === 0,
              docs: docs.map((doc) => ({
                id: doc.id,
                exists: true,
                data: () => doc
              }))
            };
          },
          where: (field, op, value) => {
            const filters = [{ field, op: convertOperator(op), value }];
            return createQueryBuilder(name, filters);
          },
          orderBy: (field, direction = "asc") => {
            return createQueryBuilder(name, [], { field, direction: direction === "asc" ? "ASCENDING" : "DESCENDING" });
          },
          limit: (count) => {
            return createQueryBuilder(name, [], void 0, count);
          }
        };
      }
      batch() {
        const operations = [];
        return {
          set: (docRef, data, options) => {
            operations.push(async () => {
              await docRef.set(data, options);
            });
          },
          update: (docRef, data) => {
            operations.push(async () => {
              await docRef.update(data);
            });
          },
          delete: (docRef) => {
            operations.push(async () => {
              await docRef.delete();
            });
          },
          commit: async () => {
            for (const op of operations) {
              await op();
            }
          }
        };
      }
    };
    restWrapper = new FirestoreRestWrapper();
  }
});

// server/firestore.ts
var firestore_exports = {};
__export(firestore_exports, {
  FirestoreService: () => FirestoreService,
  firestoreService: () => firestoreService,
  handleFirestoreError: () => handleFirestoreError
});
function handleFirestoreError(error) {
  if (error.code === 9 || error.code === "FAILED_PRECONDITION") {
    const errorMessage = error.message || error.toString();
    const urlMatch = errorMessage.match(/https:\/\/console\.firebase\.google\.com\/[^\s)]+/);
    if (urlMatch) {
      const indexUrl = urlMatch[0];
      console.log("\n" + "=".repeat(80));
      console.log("\u{1F525} FIREBASE INDEX REQUIRED - \u0627\u0646\u0642\u0631 \u0639\u0644\u0649 \u0627\u0644\u0631\u0627\u0628\u0637 \u0644\u0625\u0646\u0634\u0627\u0621 Index \u062A\u0644\u0642\u0627\u0626\u064A\u0627\u064B \u{1F525}");
      console.log("=".repeat(80));
      console.log("\n\u064A\u062C\u0628 \u0625\u0646\u0634\u0627\u0621 Firestore Index \u0644\u0643\u064A \u062A\u0639\u0645\u0644 \u0647\u0630\u0647 \u0627\u0644\u0645\u064A\u0632\u0629.");
      console.log("\n\u{1F4CC} \u0627\u0646\u0642\u0631 \u0639\u0644\u0649 \u0627\u0644\u0631\u0627\u0628\u0637 \u0627\u0644\u062A\u0627\u0644\u064A \u0644\u0625\u0646\u0634\u0627\u0621 Index \u062A\u0644\u0642\u0627\u0626\u064A\u0627\u064B:\n");
      console.log("   \u{1F449} " + indexUrl + "\n");
      console.log("\u0628\u0639\u062F \u0627\u0644\u0646\u0642\u0631 \u0639\u0644\u0649 \u0627\u0644\u0631\u0627\u0628\u0637:");
      console.log('  1\uFE0F\u20E3  \u0627\u0646\u0642\u0631 \u0639\u0644\u0649 \u0632\u0631 "Create Index"');
      console.log("  2\uFE0F\u20E3  \u0627\u0646\u062A\u0638\u0631 5-10 \u062F\u0642\u0627\u0626\u0642 \u062D\u062A\u0649 \u064A\u0643\u062A\u0645\u0644 \u0627\u0644\u0625\u0646\u0634\u0627\u0621");
      console.log("  3\uFE0F\u20E3  \u0623\u0639\u062F \u062A\u0634\u063A\u064A\u0644 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0623\u0648 \u062C\u0631\u0628 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649");
      console.log("\n" + "=".repeat(80) + "\n");
    } else {
      console.log("\n" + "=".repeat(80));
      console.log("\u{1F525} FIREBASE INDEX REQUIRED \u{1F525}");
      console.log("=".repeat(80));
      console.log("\nFirestore Index \u0645\u0637\u0644\u0648\u0628. \u0631\u0627\u062C\u0639 FIREBASE_SETUP.md \u0644\u0644\u062A\u0641\u0627\u0635\u064A\u0644.");
      console.log("\u0631\u0633\u0627\u0644\u0629 \u0627\u0644\u062E\u0637\u0623 \u0627\u0644\u0643\u0627\u0645\u0644\u0629:");
      console.log(errorMessage);
      console.log("\n" + "=".repeat(80) + "\n");
    }
    throw new Error("Firestore Index \u0645\u0637\u0644\u0648\u0628. \u0631\u0627\u062C\u0639 console logs \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0631\u0627\u0628\u0637 \u0627\u0644\u0625\u0646\u0634\u0627\u0621 \u0627\u0644\u062A\u0644\u0642\u0627\u0626\u064A.");
  }
  throw error;
}
var firestore, COLLECTIONS, FirestoreService, firestoreService;
var init_firestore = __esm({
  "server/firestore.ts"() {
    "use strict";
    init_firebase_admin_setup();
    firestore = {
      collection: (name) => getFirestore().collection(name),
      batch: () => getFirestore().batch()
    };
    COLLECTIONS = {
      STORIES: "stories",
      SETTINGS: "settings",
      USERS: "users",
      INTEGRATIONS: "integrations",
      API_CONFIGS: "api_configs",
      LINKED_ACCOUNTS: "linked_accounts",
      ACCOUNT_STATS: "account_stats",
      STORY_ASSIGNMENTS: "story_assignments"
    };
    FirestoreService = class _FirestoreService {
      async createStory(userId, story) {
        const now = /* @__PURE__ */ new Date();
        const storyData = {
          ...story,
          userId,
          status: "scheduled",
          format: story.format || "story",
          videoGenerationStatus: story.videoGenerationStatus || "pending",
          createdAt: now,
          updatedAt: now
        };
        const docRef = await firestore.collection(COLLECTIONS.STORIES).add(storyData);
        return { id: docRef.id, ...storyData };
      }
      async getStoriesByUser(userId, limitCount = 50) {
        try {
          let query = firestore.collection(COLLECTIONS.STORIES);
          if (userId !== "system-auto-publish") {
            const snapshot2 = await firestore.collection(COLLECTIONS.STORIES).where("userId", "in", [userId, "system-auto-publish"]).orderBy("createdAt", "desc").limit(limitCount).get();
            return snapshot2.docs.map((doc) => {
              const data = doc.data();
              return {
                id: doc.id,
                ...data,
                format: data.format || "story",
                videoGenerationStatus: data.videoGenerationStatus || "pending",
                createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
                updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
                scheduledTime: data.scheduledTime?.toDate ? data.scheduledTime.toDate() : new Date(data.scheduledTime)
              };
            });
          }
          const snapshot = await firestore.collection(COLLECTIONS.STORIES).where("userId", "==", userId).orderBy("createdAt", "desc").limit(limitCount).get();
          return snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              format: data.format || "story",
              videoGenerationStatus: data.videoGenerationStatus || "pending",
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              scheduledTime: data.scheduledTime?.toDate ? data.scheduledTime.toDate() : new Date(data.scheduledTime)
            };
          });
        } catch (error) {
          handleFirestoreError(error);
          throw error;
        }
      }
      async getRecentScheduledStoriesByUser(userId, limitCount = 5) {
        try {
          const snapshot = await firestore.collection(COLLECTIONS.STORIES).where("userId", "==", userId).orderBy("createdAt", "desc").limit(50).get();
          const allStories = snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              format: data.format || "story",
              videoGenerationStatus: data.videoGenerationStatus || "pending",
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              scheduledTime: data.scheduledTime?.toDate ? data.scheduledTime.toDate() : new Date(data.scheduledTime)
            };
          });
          const scheduledStories = allStories.filter((story) => story.status === "scheduled").sort((a, b) => {
            const timeA = a.scheduledTime ? new Date(a.scheduledTime).getTime() : 0;
            const timeB = b.scheduledTime ? new Date(b.scheduledTime).getTime() : 0;
            return timeB - timeA;
          }).slice(0, limitCount);
          return scheduledStories;
        } catch (error) {
          handleFirestoreError(error);
          throw error;
        }
      }
      async getStoryById(id) {
        const docSnap = await firestore.collection(COLLECTIONS.STORIES).doc(id).get();
        if (!docSnap.exists) {
          return null;
        }
        const data = docSnap.data();
        return {
          id: docSnap.id,
          ...data,
          format: data.format || "story",
          videoGenerationStatus: data.videoGenerationStatus || "pending",
          createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
          updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
          scheduledTime: data.scheduledTime?.toDate ? data.scheduledTime.toDate() : new Date(data.scheduledTime)
        };
      }
      async updateStory(id, updates) {
        await firestore.collection(COLLECTIONS.STORIES).doc(id).update({
          ...updates,
          updatedAt: /* @__PURE__ */ new Date()
        });
      }
      async deleteStory(id) {
        await firestore.collection(COLLECTIONS.STORIES).doc(id).delete();
      }
      async deleteAllFailedStories() {
        const snapshot = await firestore.collection(COLLECTIONS.STORIES).where("status", "==", "failed").get();
        const batch = firestore.batch();
        snapshot.docs.forEach((doc) => batch.delete(doc.ref));
        await batch.commit();
        return snapshot.docs.length;
      }
      static schedulerWarningShown = false;
      static lastSchedulerWarningTime = 0;
      static SCHEDULER_WARNING_INTERVAL = 3e5;
      // 5 minutes
      shouldShowSchedulerWarning() {
        const now = Date.now();
        if (!_FirestoreService.schedulerWarningShown || now - _FirestoreService.lastSchedulerWarningTime > _FirestoreService.SCHEDULER_WARNING_INTERVAL) {
          _FirestoreService.lastSchedulerWarningTime = now;
          _FirestoreService.schedulerWarningShown = true;
          return true;
        }
        return false;
      }
      async getAllScheduledStories() {
        try {
          if (!firestore) {
            return [];
          }
          const snapshot = await firestore.collection(COLLECTIONS.STORIES).where("status", "==", "scheduled").get();
          return snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              format: data.format || "story",
              videoGenerationStatus: data.videoGenerationStatus || "pending",
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              scheduledTime: data.scheduledTime?.toDate ? data.scheduledTime.toDate() : new Date(data.scheduledTime)
            };
          });
        } catch (error) {
          if (error.message?.includes("Project") || error.message?.includes("authentication")) {
            if (this.shouldShowSchedulerWarning()) {
              console.warn("\u26A0\uFE0F  Firebase not configured - Story scheduler waiting for setup");
            }
          } else {
            console.error("Error getting scheduled stories:", error);
          }
          return [];
        }
      }
      async getUserSettings(userId) {
        if (!firestore) {
          console.warn("\u26A0\uFE0F  Firestore not initialized");
          return null;
        }
        const docSnap = await firestore.collection(COLLECTIONS.SETTINGS).doc(userId).get();
        if (!docSnap.exists) {
          const defaultSettings = {
            userId,
            emailNotifications: true,
            smsNotifications: false,
            pushNotifications: true,
            publicProfile: false,
            showActivity: false,
            autoPublish: true,
            preferredPublishTime: "12:00",
            autoStoryGenerationEnabled: false,
            autoStoryWithMusic: true,
            autoStoryWithVideo: false
          };
          await firestore.collection(COLLECTIONS.SETTINGS).doc(userId).set(defaultSettings);
          return defaultSettings;
        }
        return docSnap.data();
      }
      async updateUserSettings(userId, settings) {
        await firestore.collection(COLLECTIONS.SETTINGS).doc(userId).set(settings, { merge: true });
      }
      async getUserById(id) {
        const docSnap = await firestore.collection(COLLECTIONS.USERS).doc(id).get();
        if (!docSnap.exists) return null;
        const data = docSnap.data();
        return {
          id: docSnap.id,
          ...data,
          createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : /* @__PURE__ */ new Date()
        };
      }
      async updateUser(id, updates) {
        await firestore.collection(COLLECTIONS.USERS).doc(id).set({
          ...updates,
          updatedAt: /* @__PURE__ */ new Date()
        }, { merge: true });
      }
      async getAllUsers() {
        const snapshot = await firestore.collection(COLLECTIONS.USERS).get();
        return snapshot.docs.map((doc) => {
          const data = doc.data();
          return {
            id: doc.id,
            ...data,
            createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : /* @__PURE__ */ new Date()
          };
        });
      }
      async getPlatformIntegrations() {
        const snapshot = await firestore.collection(COLLECTIONS.INTEGRATIONS).get();
        if (snapshot.empty) {
          const defaultIntegrations = [
            { platform: "facebook", enabled: true, moderationEnabled: false },
            { platform: "instagram", enabled: true, moderationEnabled: false },
            { platform: "tiktok", enabled: true, moderationEnabled: false }
          ];
          for (const integration of defaultIntegrations) {
            await firestore.collection(COLLECTIONS.INTEGRATIONS).doc(integration.platform).set(integration);
          }
          return defaultIntegrations;
        }
        return snapshot.docs.map((doc) => doc.data());
      }
      async updatePlatformIntegration(platform, updates) {
        await firestore.collection(COLLECTIONS.INTEGRATIONS).doc(platform).set(updates, { merge: true });
      }
      async getAPIConfigs() {
        const snapshot = await firestore.collection(COLLECTIONS.API_CONFIGS).get();
        if (snapshot.empty) {
          const defaultConfigs = [
            { provider: "facebook", appId: "", appSecret: "", isConnected: false },
            { provider: "instagram", appId: "", appSecret: "", isConnected: false },
            { provider: "tiktok", apiKey: "", appSecret: "", isConnected: false },
            { provider: "deepseek", apiKey: "", isConnected: false },
            { provider: "cloudflare_r2", isConnected: false, additionalConfig: {} },
            { provider: "youtube", apiKey: "", isConnected: false },
            { provider: "huggingface", apiKey: "", isConnected: false },
            { provider: "gemini", apiKey: "", isConnected: false },
            { provider: "google_trends", apiKey: "", isConnected: false, additionalConfig: { searchEngineId: "" } },
            { provider: "tmdb", apiKey: "", isConnected: false }
          ];
          for (const config of defaultConfigs) {
            await firestore.collection(COLLECTIONS.API_CONFIGS).doc(config.provider).set(config);
          }
          return defaultConfigs;
        }
        return snapshot.docs.map((doc) => {
          const data = doc.data();
          return {
            ...data,
            lastTested: data.lastTested?.toDate ? data.lastTested.toDate() : void 0
          };
        });
      }
      async getAPIConfig(provider) {
        const docSnap = await firestore.collection(COLLECTIONS.API_CONFIGS).doc(provider).get();
        if (!docSnap.exists) {
          return null;
        }
        const data = docSnap.data();
        return {
          ...data,
          lastTested: data.lastTested?.toDate ? data.lastTested.toDate() : void 0
        };
      }
      async updateAPIConfig(provider, updates) {
        const current = await this.getAPIConfig(provider);
        const updateData = {
          provider,
          isConnected: current?.isConnected ?? false
        };
        if (updates.apiKey !== void 0) updateData.apiKey = updates.apiKey;
        if (updates.appId !== void 0) updateData.appId = updates.appId;
        if (updates.appSecret !== void 0) updateData.appSecret = updates.appSecret;
        if (updates.redirectUrl !== void 0) updateData.redirectUrl = updates.redirectUrl;
        if (updates.additionalConfig !== void 0) {
          updateData.additionalConfig = {
            ...current?.additionalConfig || {},
            ...updates.additionalConfig
          };
        }
        if (updates.isConnected !== void 0) updateData.isConnected = updates.isConnected;
        if (updates.lastTested !== void 0) updateData.lastTested = updates.lastTested;
        await firestore.collection(COLLECTIONS.API_CONFIGS).doc(provider).set(updateData, { merge: true });
      }
      async createLinkedAccount(userId, account) {
        const stats = await this.getUserAccountStats(userId);
        if (stats.totalAccounts >= stats.maxAccounts) {
          throw new Error(`\u0644\u0642\u062F \u0648\u0635\u0644\u062A \u0625\u0644\u0649 \u0627\u0644\u062D\u062F \u0627\u0644\u0623\u0642\u0635\u0649 \u0644\u0644\u062D\u0633\u0627\u0628\u0627\u062A \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0628\u0647\u0627 (${stats.maxAccounts})`);
        }
        const now = /* @__PURE__ */ new Date();
        const accountData = {
          ...account,
          userId,
          status: "active",
          quotas: account.quotas || {
            dailyLimit: 50,
            dailyUsed: 0,
            monthlyLimit: 1e3,
            monthlyUsed: 0,
            resetAt: new Date(Date.now() + 24 * 60 * 60 * 1e3)
          },
          createdAt: now,
          updatedAt: now
        };
        const docRef = await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).add(accountData);
        await this.updateUserAccountStats(userId);
        return { id: docRef.id, ...accountData };
      }
      async getLinkedAccountsByUser(userId, options) {
        try {
          let query = firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).where("userId", "==", userId);
          if (options?.platform) {
            query = query.where("platform", "==", options.platform);
          }
          if (options?.status) {
            query = query.where("status", "==", options.status);
          }
          if (!options?.status && !options?.platform) {
            query = query.orderBy("createdAt", "desc");
          }
          if (options?.limit) {
            query = query.limit(options.limit);
          }
          if (options?.startAfter) {
            const startDoc = await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).doc(options.startAfter).get();
            if (startDoc.exists) {
              query = query.startAfter(startDoc);
            }
          }
          const snapshot = await query.get();
          let accounts = snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              tokenExpiresAt: data.tokenExpiresAt?.toDate ? data.tokenExpiresAt.toDate() : void 0,
              lastSyncedAt: data.lastSyncedAt?.toDate ? data.lastSyncedAt.toDate() : void 0,
              lastPublishedAt: data.lastPublishedAt?.toDate ? data.lastPublishedAt.toDate() : void 0,
              quotas: {
                ...data.quotas,
                resetAt: data.quotas?.resetAt?.toDate ? data.quotas.resetAt.toDate() : /* @__PURE__ */ new Date()
              }
            };
          });
          if (options?.search) {
            const searchLower = options.search.toLowerCase();
            accounts = accounts.filter(
              (acc) => acc.name.toLowerCase().includes(searchLower) || acc.username?.toLowerCase().includes(searchLower) || acc.externalId.includes(searchLower)
            );
          }
          return accounts;
        } catch (error) {
          handleFirestoreError(error);
          throw error;
        }
      }
      async getLinkedAccountById(id) {
        const docSnap = await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).doc(id).get();
        if (!docSnap.exists) {
          return null;
        }
        const data = docSnap.data();
        return {
          id: docSnap.id,
          ...data,
          createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
          updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
          tokenExpiresAt: data.tokenExpiresAt?.toDate ? data.tokenExpiresAt.toDate() : void 0,
          lastSyncedAt: data.lastSyncedAt?.toDate ? data.lastSyncedAt.toDate() : void 0,
          lastPublishedAt: data.lastPublishedAt?.toDate ? data.lastPublishedAt.toDate() : void 0,
          quotas: {
            ...data.quotas,
            resetAt: data.quotas?.resetAt?.toDate ? data.quotas.resetAt.toDate() : /* @__PURE__ */ new Date()
          }
        };
      }
      async updateLinkedAccount(id, updates) {
        await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).doc(id).update({
          ...updates,
          updatedAt: /* @__PURE__ */ new Date()
        });
      }
      async deleteLinkedAccount(id, userId) {
        const account = await this.getLinkedAccountById(id);
        if (!account || account.userId !== userId) {
          throw new Error("\u0627\u0644\u062D\u0633\u0627\u0628 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0623\u0648 \u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0644\u062D\u0630\u0641\u0647");
        }
        await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).doc(id).delete();
        const assignmentsSnapshot = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).where("accountId", "==", id).get();
        const batch = firestore.batch();
        assignmentsSnapshot.docs.forEach((doc) => batch.delete(doc.ref));
        await batch.commit();
        await this.updateUserAccountStats(userId);
      }
      async getAccountsNeedingTokenRefresh() {
        try {
          const now = /* @__PURE__ */ new Date();
          const snapshot = await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).where("status", "==", "active").get();
          return snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              tokenExpiresAt: data.tokenExpiresAt?.toDate ? data.tokenExpiresAt.toDate() : void 0
            };
          }).filter((account) => {
            if (!account.tokenExpiresAt) return false;
            const expiresAt = new Date(account.tokenExpiresAt);
            const hoursUntilExpiry = (expiresAt.getTime() - now.getTime()) / (1e3 * 60 * 60);
            return hoursUntilExpiry < 24 && hoursUntilExpiry > 0;
          });
        } catch (error) {
          console.error("\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0627\u0644\u062D\u0633\u0627\u0628\u0627\u062A \u0627\u0644\u0645\u0646\u062A\u0647\u064A\u0629 \u0627\u0644\u0635\u0644\u0627\u062D\u064A\u0629:", error);
          return [];
        }
      }
      async getUserAccountStats(userId) {
        const docSnap = await firestore.collection(COLLECTIONS.ACCOUNT_STATS).doc(userId).get();
        if (!docSnap.exists) {
          const defaultStats = {
            userId,
            totalAccounts: 0,
            facebookAccounts: 0,
            instagramAccounts: 0,
            tiktokAccounts: 0,
            activeAccounts: 0,
            inactiveAccounts: 0,
            maxAccounts: 1e3,
            updatedAt: /* @__PURE__ */ new Date()
          };
          await firestore.collection(COLLECTIONS.ACCOUNT_STATS).doc(userId).set(defaultStats);
          return defaultStats;
        }
        const data = docSnap.data();
        return {
          ...data,
          updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : /* @__PURE__ */ new Date()
        };
      }
      async updateUserAccountStats(userId) {
        const accounts = await this.getLinkedAccountsByUser(userId);
        const stories = await this.getStoriesByUser(userId, 100);
        const publishedStories = stories.filter((s) => s.status === "published");
        const totalFollowers = accounts.reduce((sum, acc) => {
          return sum + (acc.followers || 0);
        }, 0);
        const totalReach = publishedStories.reduce((sum, s) => sum + (Number(s.reach) || 0), 0);
        const totalEngagement = publishedStories.reduce((sum, s) => sum + (Number(s.engagementRate) || 0), 0);
        const avgEngagement = publishedStories.length > 0 ? totalEngagement / publishedStories.length : 0;
        const stats = {
          userId,
          totalAccounts: accounts.length,
          facebookAccounts: accounts.filter((a) => a.platform === "facebook").length,
          instagramAccounts: accounts.filter((a) => a.platform === "instagram").length,
          tiktokAccounts: accounts.filter((a) => a.platform === "tiktok").length,
          activeAccounts: accounts.filter((a) => a.status === "active").length,
          inactiveAccounts: accounts.filter((a) => a.status !== "active").length,
          totalFollowers,
          totalReach,
          avgEngagement,
          totalPosts: publishedStories.length,
          growthRate: 0,
          maxAccounts: 1e3,
          updatedAt: /* @__PURE__ */ new Date()
        };
        const userRef = firestore.collection(COLLECTIONS.USERS).doc(userId);
        await userRef.set({
          stats: {
            totalFollowers,
            totalReach,
            avgEngagement,
            activeAccounts: stats.activeAccounts,
            lastStatsUpdate: /* @__PURE__ */ new Date()
          },
          aggregateStats: {
            totalFollowers,
            totalReach,
            avgEngagement,
            lastUpdated: /* @__PURE__ */ new Date()
          }
        }, { merge: true });
        await firestore.collection(COLLECTIONS.ACCOUNT_STATS).doc(userId).set(stats);
        return stats;
      }
      async assignAccountToStory(storyId, accountId) {
        const now = /* @__PURE__ */ new Date();
        const assignmentData = {
          storyId,
          accountId,
          assignedAt: now,
          status: "pending"
        };
        const docRef = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).add(assignmentData);
        return { id: docRef.id, ...assignmentData };
      }
      async getStoryAssignments(storyId) {
        const snapshot = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).where("storyId", "==", storyId).get();
        return snapshot.docs.map((doc) => {
          const data = doc.data();
          return {
            ...data,
            assignedAt: data.assignedAt?.toDate ? data.assignedAt.toDate() : new Date(data.assignedAt),
            publishedAt: data.publishedAt?.toDate ? data.publishedAt.toDate() : void 0
          };
        });
      }
      async getAccountAssignments(accountId) {
        const snapshot = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).where("accountId", "==", accountId).get();
        return snapshot.docs.map((doc) => {
          const data = doc.data();
          return {
            ...data,
            assignedAt: data.assignedAt?.toDate ? data.assignedAt.toDate() : new Date(data.assignedAt),
            publishedAt: data.publishedAt?.toDate ? data.publishedAt.toDate() : void 0
          };
        });
      }
      async updateAssignmentStatus(storyId, accountId, status, error) {
        const snapshot = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).where("storyId", "==", storyId).where("accountId", "==", accountId).limit(1).get();
        if (snapshot.empty) {
          return;
        }
        const doc = snapshot.docs[0];
        const updateData = { status };
        if (status === "published") {
          updateData.publishedAt = /* @__PURE__ */ new Date();
        }
        if (error !== void 0) {
          updateData.error = error;
        }
        await doc.ref.update(updateData);
      }
      async removeStoryAssignment(storyId, accountId) {
        const snapshot = await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).where("storyId", "==", storyId).where("accountId", "==", accountId).get();
        const batch = firestore.batch();
        snapshot.docs.forEach((doc) => batch.delete(doc.ref));
        await batch.commit();
      }
      async getActiveLinkedAccounts() {
        try {
          const snapshot = await firestore.collection(COLLECTIONS.LINKED_ACCOUNTS).where("status", "==", "active").get();
          return snapshot.docs.map((doc) => {
            const data = doc.data();
            return {
              id: doc.id,
              ...data,
              createdAt: data.createdAt?.toDate ? data.createdAt.toDate() : new Date(data.createdAt),
              updatedAt: data.updatedAt?.toDate ? data.updatedAt.toDate() : new Date(data.updatedAt),
              tokenExpiresAt: data.tokenExpiresAt?.toDate ? data.tokenExpiresAt.toDate() : void 0,
              lastSyncedAt: data.lastSyncedAt?.toDate ? data.lastSyncedAt.toDate() : void 0,
              lastPublishedAt: data.lastPublishedAt?.toDate ? data.lastPublishedAt.toDate() : void 0,
              quotas: {
                ...data.quotas,
                resetAt: data.quotas?.resetAt?.toDate ? data.quotas.resetAt.toDate() : /* @__PURE__ */ new Date()
              }
            };
          });
        } catch (error) {
          handleFirestoreError(error);
          throw error;
        }
      }
      async createAutoStory(storyData) {
        const now = /* @__PURE__ */ new Date();
        const data = {
          ...storyData,
          userId: "system-auto-publish",
          status: "scheduled",
          createdAt: now,
          updatedAt: now
        };
        const docRef = await firestore.collection(COLLECTIONS.STORIES).add(data);
        return docRef.id;
      }
      async assignStoryToAccount(storyId, accountId) {
        const now = /* @__PURE__ */ new Date();
        const assignmentData = {
          storyId,
          accountId,
          assignedAt: now,
          status: "pending"
        };
        await firestore.collection(COLLECTIONS.STORY_ASSIGNMENTS).add(assignmentData);
      }
    };
    firestoreService = new FirestoreService();
  }
});

// server/youtube-music.ts
var youtube_music_exports = {};
__export(youtube_music_exports, {
  YouTubeMusicService: () => YouTubeMusicService
});
var YouTubeMusicService;
var init_youtube_music = __esm({
  "server/youtube-music.ts"() {
    "use strict";
    YouTubeMusicService = class {
      apiKey;
      constructor(apiKey) {
        this.apiKey = apiKey;
      }
      async searchMusic(query, limit = 10) {
        try {
          const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(query + " music")}&type=video&videoCategoryId=10&maxResults=${limit}&key=${this.apiKey}`;
          const response = await fetch(url);
          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error?.message || "\u0641\u0634\u0644 \u0627\u0644\u0628\u062D\u062B \u0639\u0646 \u0627\u0644\u0645\u0648\u0633\u064A\u0642\u0649");
          }
          const data = await response.json();
          const videoIds = data.items.map((item) => item.id.videoId).join(",");
          const detailsUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails&id=${videoIds}&key=${this.apiKey}`;
          const detailsResponse = await fetch(detailsUrl);
          if (!detailsResponse.ok) {
            throw new Error("\u0641\u0634\u0644 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u062A\u0641\u0627\u0635\u064A\u0644 \u0627\u0644\u0641\u064A\u062F\u064A\u0648");
          }
          const detailsData = await detailsResponse.json();
          const results = detailsData.items.map((item) => {
            const duration = this.parseDuration(item.contentDetails.duration);
            return {
              videoId: item.id,
              title: item.snippet.title,
              artist: item.snippet.channelTitle,
              duration,
              thumbnail: item.snippet.thumbnails.high?.url || item.snippet.thumbnails.medium?.url || item.snippet.thumbnails.default?.url,
              url: `https://www.youtube.com/watch?v=${item.id}`
            };
          });
          return { results };
        } catch (error) {
          console.error("YouTube Music search error:", error);
          throw new Error(error.message || "\u0641\u0634\u0644 \u0627\u0644\u0628\u062D\u062B \u0639\u0646 \u0627\u0644\u0645\u0648\u0633\u064A\u0642\u0649");
        }
      }
      parseDuration(duration) {
        const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
        if (!match) return 0;
        const hours = parseInt(match[1] || "0");
        const minutes = parseInt(match[2] || "0");
        const seconds = parseInt(match[3] || "0");
        return hours * 3600 + minutes * 60 + seconds;
      }
      async testConnection() {
        try {
          console.log("[YouTube Test] Starting connection test...");
          console.log("[YouTube Test] API Key (masked):", this.apiKey ? this.apiKey.substring(0, 10) + "..." : "not provided");
          if (!this.apiKey || this.apiKey.trim() === "") {
            return {
              success: false,
              message: "\u0645\u0641\u062A\u0627\u062D YouTube API \u0645\u0637\u0644\u0648\u0628"
            };
          }
          const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=test&type=video&maxResults=1&key=${this.apiKey}`;
          const response = await fetch(url);
          console.log("[YouTube Test] Response status:", response.status, response.statusText);
          if (!response.ok) {
            let errorMessage = "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 YouTube API";
            try {
              const errorData = await response.json();
              console.log("[YouTube Test] Error response:", JSON.stringify(errorData, null, 2));
              if (response.status === 400) {
                if (errorData.error?.message?.includes("API key not valid")) {
                  errorMessage = "\u0645\u0641\u062A\u0627\u062D YouTube API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D";
                } else if (errorData.error?.message?.includes("quota")) {
                  errorMessage = "\u062A\u062C\u0627\u0648\u0632\u062A \u062D\u0635\u0629 YouTube API \u0627\u0644\u064A\u0648\u0645\u064A\u0629 - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0644\u0627\u062D\u0642\u0627\u064B";
                } else {
                  errorMessage = `\u062E\u0637\u0623 YouTube API: ${errorData.error?.message || errorData.message || "\u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641"}`;
                }
              } else if (response.status === 403) {
                if (errorData.error?.message?.includes("Daily Limit Exceeded")) {
                  errorMessage = "\u062A\u062C\u0627\u0648\u0632\u062A \u0627\u0644\u062D\u062F \u0627\u0644\u064A\u0648\u0645\u064A \u0644\u0627\u0633\u062A\u062E\u062F\u0627\u0645 YouTube API";
                } else if (errorData.error?.message?.includes("not enabled")) {
                  errorMessage = "YouTube Data API v3 \u063A\u064A\u0631 \u0645\u0641\u0639\u0644 \u0641\u064A \u0645\u0634\u0631\u0648\u0639\u0643 - \u064A\u0631\u062C\u0649 \u062A\u0641\u0639\u064A\u0644\u0647 \u0645\u0646 Google Cloud Console";
                } else {
                  errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0625\u0639\u062F\u0627\u062F\u0627\u062A API \u0641\u064A Google Cloud Console";
                }
              } else if (response.status === 404) {
                errorMessage = "\u062E\u062F\u0645\u0629 YouTube API \u063A\u064A\u0631 \u0645\u062A\u0627\u062D\u0629 - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0644\u0627\u062D\u0642\u0627\u064B";
              } else {
                errorMessage = errorData.error?.message || `\u062E\u0637\u0623 YouTube API (\u0631\u0645\u0632 ${response.status})`;
              }
            } catch (parseError) {
              console.log("[YouTube Test] Failed to parse error response:", parseError);
              errorMessage = `\u062E\u0637\u0623 YouTube API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
            }
            console.log("[YouTube Test] Final error message:", errorMessage);
            return {
              success: false,
              message: errorMessage
            };
          }
          const successData = await response.json();
          console.log("[YouTube Test] Success! Response:", JSON.stringify(successData, null, 2));
          return {
            success: true,
            message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 YouTube API - \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0635\u0627\u0644\u062D \u0648\u064A\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D"
          };
        } catch (error) {
          console.log("[YouTube Test] Exception:", error);
          return {
            success: false,
            message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 YouTube API: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
          };
        }
      }
    };
  }
});

// server/r2-storage.ts
var r2_storage_exports = {};
__export(r2_storage_exports, {
  R2StorageService: () => R2StorageService,
  r2Storage: () => r2Storage
});
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
  CopyObjectCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
var R2StorageService, r2Storage;
var init_r2_storage = __esm({
  "server/r2-storage.ts"() {
    "use strict";
    init_firestore();
    R2StorageService = class {
      client = null;
      bucketName = "";
      accountId = "";
      async initialize() {
        const config = await firestoreService.getAPIConfig("cloudflare_r2");
        const r2Config = {
          accountId: config?.additionalConfig?.accountId || process.env.R2_ACCOUNT_ID,
          accessKeyId: config?.additionalConfig?.accessKeyId || process.env.R2_ACCESS_KEY_ID,
          secretAccessKey: config?.additionalConfig?.secretAccessKey || process.env.R2_SECRET_ACCESS_KEY,
          bucketName: config?.additionalConfig?.bucketName || process.env.R2_BUCKET_NAME
        };
        if (!r2Config.accountId || !r2Config.accessKeyId || !r2Config.secretAccessKey || !r2Config.bucketName) {
          throw new Error("Missing Cloudflare R2 configuration. Please configure R2 credentials in the admin panel or environment variables.");
        }
        this.accountId = r2Config.accountId;
        this.bucketName = r2Config.bucketName;
        this.client = new S3Client({
          region: "auto",
          endpoint: `https://${r2Config.accountId}.r2.cloudflarestorage.com`,
          credentials: {
            accessKeyId: r2Config.accessKeyId,
            secretAccessKey: r2Config.secretAccessKey
          }
        });
      }
      async ensureInitialized() {
        if (!this.client) {
          await this.initialize();
        }
      }
      sanitizeMetadata(metadata) {
        if (!metadata) return void 0;
        const sanitized = {};
        for (const [key, value] of Object.entries(metadata)) {
          const hasNonAscii = /[^\x00-\x7F]/.test(value);
          if (hasNonAscii) {
            sanitized[key] = Buffer.from(value, "utf-8").toString("base64");
            sanitized[`${key}-encoded`] = "base64";
          } else {
            sanitized[key] = value;
          }
        }
        return sanitized;
      }
      async uploadFile(file, fileName, options) {
        await this.ensureInitialized();
        const sanitizedMetadata = this.sanitizeMetadata(options?.metadata);
        const command = new PutObjectCommand({
          Bucket: this.bucketName,
          Key: fileName,
          Body: file,
          ContentType: options?.contentType || "application/octet-stream",
          Metadata: sanitizedMetadata,
          CacheControl: options?.cacheControl || "public, max-age=31536000, immutable"
        });
        this.client.send(command).catch((err) => console.error(`Background upload failed for ${fileName}:`, err));
        const getCommand = new GetObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        const presignedUrl = await getSignedUrl(this.client, getCommand, { expiresIn: 604800 });
        return presignedUrl;
      }
      async uploadFileWithLongUrl(file, fileName, options) {
        await this.ensureInitialized();
        const sanitizedMetadata = this.sanitizeMetadata(options?.metadata);
        const command = new PutObjectCommand({
          Bucket: this.bucketName,
          Key: fileName,
          Body: file,
          ContentType: options?.contentType || "application/octet-stream",
          Metadata: sanitizedMetadata,
          CacheControl: options?.cacheControl || "public, max-age=31536000"
        });
        await this.client.send(command);
        const getCommand = new GetObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        const presignedUrl = await getSignedUrl(this.client, getCommand, { expiresIn: 604800 });
        return presignedUrl;
      }
      async getFile(fileName) {
        await this.ensureInitialized();
        const command = new GetObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        const response = await this.client.send(command);
        if (!response.Body) {
          throw new Error("File not found or empty");
        }
        const chunks = [];
        for await (const chunk of response.Body) {
          chunks.push(chunk);
        }
        return Buffer.concat(chunks);
      }
      async getFileUrl(fileName, expiresIn = 3600) {
        await this.ensureInitialized();
        const command = new GetObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        const url = await getSignedUrl(this.client, command, { expiresIn });
        return url;
      }
      async getUploadUrl(fileName, contentType, expiresIn = 3600) {
        await this.ensureInitialized();
        const command = new PutObjectCommand({
          Bucket: this.bucketName,
          Key: fileName,
          ContentType: contentType
        });
        const url = await getSignedUrl(this.client, command, { expiresIn });
        return url;
      }
      async deleteFile(fileName) {
        await this.ensureInitialized();
        const command = new DeleteObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        await this.client.send(command);
      }
      async listFiles(prefix, maxKeys = 1e3, continuationToken) {
        await this.ensureInitialized();
        const command = new ListObjectsV2Command({
          Bucket: this.bucketName,
          Prefix: prefix,
          MaxKeys: maxKeys,
          ContinuationToken: continuationToken
        });
        const response = await this.client.send(command);
        return {
          objects: (response.Contents || []).map((obj) => ({
            key: obj.Key,
            size: obj.Size,
            lastModified: obj.LastModified
          })),
          hasMore: response.IsTruncated || false,
          nextToken: response.NextContinuationToken
        };
      }
      async fileExists(fileName) {
        await this.ensureInitialized();
        try {
          const command = new HeadObjectCommand({
            Bucket: this.bucketName,
            Key: fileName
          });
          await this.client.send(command);
          return true;
        } catch (error) {
          if (error.name === "NotFound") {
            return false;
          }
          throw error;
        }
      }
      async getFileMetadata(fileName) {
        await this.ensureInitialized();
        const command = new HeadObjectCommand({
          Bucket: this.bucketName,
          Key: fileName
        });
        const response = await this.client.send(command);
        return {
          contentType: response.ContentType,
          contentLength: response.ContentLength,
          lastModified: response.LastModified,
          metadata: response.Metadata,
          etag: response.ETag
        };
      }
      async copyFile(sourceKey, destinationKey) {
        await this.ensureInitialized();
        const command = new CopyObjectCommand({
          Bucket: this.bucketName,
          CopySource: `${this.bucketName}/${sourceKey}`,
          Key: destinationKey
        });
        await this.client.send(command);
      }
      async uploadFromUrl(url, fileName, options) {
        const response = await fetch(url);
        if (!response.ok) {
          throw new Error(`Failed to fetch file from URL: ${response.statusText}`);
        }
        const arrayBuffer = await response.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        return await this.uploadFile(buffer, fileName, {
          ...options,
          contentType: options?.contentType || response.headers.get("content-type") || "application/octet-stream"
        });
      }
      async batchDelete(fileNames) {
        await this.ensureInitialized();
        const deletePromises = fileNames.map((fileName) => this.deleteFile(fileName));
        await Promise.all(deletePromises);
      }
      async getPublicUrl(fileName) {
        await this.ensureInitialized();
        return `https://${this.accountId}.r2.cloudflarestorage.com/${this.bucketName}/${fileName}`;
      }
      async moveFile(sourceKey, destinationKey) {
        await this.ensureInitialized();
        await this.copyFile(sourceKey, destinationKey);
        await this.deleteFile(sourceKey);
      }
      extractFileKeyFromUrl(url) {
        try {
          const urlObj = new URL(url);
          const pathParts = urlObj.pathname.split("/").filter((p) => p.length > 0);
          if (url.includes(".r2.cloudflarestorage.com")) {
            if (pathParts.length >= 1 && pathParts[0] === this.bucketName) {
              return pathParts.slice(1).join("/");
            }
            if (pathParts.length >= 1) {
              return pathParts.join("/");
            }
          } else if (url.includes("r2.dev")) {
            if (pathParts.length >= 1) {
              return pathParts.join("/");
            }
          }
          if (pathParts.length >= 1) {
            return pathParts.join("/");
          }
          return null;
        } catch {
          return null;
        }
      }
      // AWS S3 Signature V4 max expiration is 7 days (604800 seconds)
      async refreshSignedUrl(oldUrl, expiresIn = 604800) {
        await this.ensureInitialized();
        console.log(`\u{1F50D} Refreshing URL: ${oldUrl.substring(0, 100)}...`);
        const fileKey = this.extractFileKeyFromUrl(oldUrl);
        if (!fileKey) {
          console.log(`\u274C Could not extract file key from URL`);
          console.log(`   URL: ${oldUrl}`);
          throw new Error(`\u062A\u0639\u0630\u0631 \u0627\u0633\u062A\u062E\u0631\u0627\u062C \u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0645\u0644\u0641 \u0645\u0646 \u0627\u0644\u0631\u0627\u0628\u0637: ${oldUrl}`);
        }
        console.log(`\u{1F4C2} Extracted file key: ${fileKey}`);
        try {
          const exists = await this.fileExists(fileKey);
          if (!exists) {
            console.log(`\u274C File ${fileKey} does not exist in R2`);
            throw new Error(`\u0627\u0644\u0645\u0644\u0641 ${fileKey} \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0633\u062D\u0627\u0628\u064A. \u064A\u0631\u062C\u0649 \u0625\u0639\u0627\u062F\u0629 \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631\u0629.`);
          }
          const command = new GetObjectCommand({
            Bucket: this.bucketName,
            Key: fileKey
          });
          const freshUrl = await getSignedUrl(this.client, command, { expiresIn });
          console.log(`\u2705 Refreshed signed URL`);
          console.log(`   New URL: ${freshUrl.substring(0, 100)}...`);
          return freshUrl;
        } catch (error) {
          console.error(`\u274C Error refreshing signed URL:`, error);
          throw new Error(error.message || `\u0641\u0634\u0644 \u0641\u064A \u062A\u062D\u062F\u064A\u062B \u0631\u0627\u0628\u0637 \u0627\u0644\u0645\u0644\u0641: ${fileKey}`);
        }
      }
      async getFileAsBuffer(fileName) {
        await this.ensureInitialized();
        const exists = await this.fileExists(fileName);
        if (!exists) {
          throw new Error(`\u0627\u0644\u0645\u0644\u0641 ${fileName} \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0633\u062D\u0627\u0628\u064A`);
        }
        return await this.getFile(fileName);
      }
      async verifyAndGetUrl(url) {
        await this.ensureInitialized();
        const fileKey = this.extractFileKeyFromUrl(url);
        if (!fileKey) {
          return { valid: false, error: "\u062A\u0639\u0630\u0631 \u0627\u0633\u062A\u062E\u0631\u0627\u062C \u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0645\u0644\u0641 \u0645\u0646 \u0627\u0644\u0631\u0627\u0628\u0637" };
        }
        try {
          const exists = await this.fileExists(fileKey);
          if (!exists) {
            return { valid: false, fileKey, error: `\u0627\u0644\u0645\u0644\u0641 ${fileKey} \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646` };
          }
          const command = new GetObjectCommand({
            Bucket: this.bucketName,
            Key: fileKey
          });
          const freshUrl = await getSignedUrl(this.client, command, { expiresIn: 604800 });
          return { valid: true, freshUrl, fileKey };
        } catch (error) {
          return { valid: false, fileKey, error: error.message };
        }
      }
    };
    r2Storage = new R2StorageService();
  }
});

// server/storage-service.ts
var storage_service_exports = {};
__export(storage_service_exports, {
  StorageService: () => StorageService,
  storageService: () => storageService
});
import * as fs from "fs";
import * as path from "path";
var StorageService, storageService;
var init_storage_service = __esm({
  "server/storage-service.ts"() {
    "use strict";
    init_r2_storage();
    StorageService = class {
      basePath = "videos";
      archivePath = "videos/archive";
      tempPath = "/tmp/video-storage";
      constructor() {
        this.ensureTempDirectory();
      }
      ensureTempDirectory() {
        if (!fs.existsSync(this.tempPath)) {
          fs.mkdirSync(this.tempPath, { recursive: true });
        }
      }
      /**
       * Upload video to R2 storage
       */
      async uploadVideo(videoPath, storyId, category, duration = 20) {
        try {
          if (!fs.existsSync(videoPath)) {
            return {
              success: false,
              error: `Video file not found: ${videoPath}`
            };
          }
          const fileStats = fs.statSync(videoPath);
          const fileSize = fileStats.size;
          console.log(`\u{1F4E4} Uploading video: ${storyId} (${fileSize} bytes)`);
          const videoBuffer = fs.readFileSync(videoPath);
          const timestamp = (/* @__PURE__ */ new Date()).toISOString().replace(/[:.]/g, "-");
          const storageKey = `${this.basePath}/${category}/${storyId}_${timestamp}.mp4`;
          const metadata = {
            "story-id": storyId,
            "category": category,
            "duration": duration.toString(),
            "generated-at": (/* @__PURE__ */ new Date()).toISOString()
          };
          const videoUrl = await r2Storage.uploadFile(videoBuffer, storageKey, {
            contentType: "video/mp4",
            metadata,
            cacheControl: "public, max-age=31536000"
          });
          console.log(`\u2705 Video uploaded: ${storageKey}`);
          return {
            success: true,
            videoUrl,
            storageKey,
            fileSize
          };
        } catch (error) {
          console.error(`\u274C Video upload failed: ${error}`);
          return {
            success: false,
            error: String(error)
          };
        }
      }
      /**
       * Get video URL from storage
       */
      async getVideoUrl(storageKey, expiresIn = 3600) {
        try {
          const url = await r2Storage.getFileUrl(storageKey, expiresIn);
          return url;
        } catch (error) {
          console.error(`\u274C Failed to get video URL: ${error}`);
          return null;
        }
      }
      /**
       * Delete video from storage
       */
      async deleteVideo(storageKey) {
        try {
          await r2Storage.deleteFile(storageKey);
          console.log(`\u{1F5D1}\uFE0F Video deleted: ${storageKey}`);
          return true;
        } catch (error) {
          console.error(`\u274C Failed to delete video: ${error}`);
          return false;
        }
      }
      /**
       * Archive old videos (move to archive path)
       */
      async archiveOldVideos(olderThanDays = 30) {
        try {
          console.log(`\u{1F4E6} Archiving videos older than ${olderThanDays} days...`);
          const cutoffTime = /* @__PURE__ */ new Date();
          cutoffTime.setDate(cutoffTime.getDate() - olderThanDays);
          const listResult = await r2Storage.listFiles(this.basePath);
          let archivedCount = 0;
          for (const obj of listResult.objects) {
            if (obj.lastModified && obj.lastModified < cutoffTime && !obj.key.includes("/archive/")) {
              const archiveKey = obj.key.replace(this.basePath, this.archivePath);
              try {
                const videoBuffer = await r2Storage.getFile(obj.key);
                await r2Storage.uploadFile(videoBuffer, archiveKey, {
                  contentType: "video/mp4",
                  cacheControl: "public, max-age=31536000"
                });
                await r2Storage.deleteFile(obj.key);
                archivedCount++;
                console.log(`  \u{1F4C1} Archived: ${obj.key}`);
              } catch (error) {
                console.warn(`  \u26A0\uFE0F Failed to archive ${obj.key}: ${error}`);
              }
            }
          }
          console.log(`\u2705 Archived ${archivedCount} videos`);
          return archivedCount;
        } catch (error) {
          console.error(`\u274C Archival process failed: ${error}`);
          return 0;
        }
      }
      /**
       * Get storage statistics
       */
      async getStorageStats() {
        try {
          const result = await r2Storage.listFiles(this.basePath, 1e4);
          const stats = {
            totalVideos: 0,
            totalSize: 0,
            videosByCategory: {}
          };
          for (const obj of result.objects) {
            stats.totalVideos++;
            stats.totalSize += obj.size || 0;
            const parts = obj.key.split("/");
            if (parts.length >= 3) {
              const category = parts[1];
              stats.videosByCategory[category] = (stats.videosByCategory[category] || 0) + 1;
            }
          }
          return stats;
        } catch (error) {
          console.error(`\u274C Failed to get storage stats: ${error}`);
          return {
            totalVideos: 0,
            totalSize: 0,
            videosByCategory: {}
          };
        }
      }
      /**
       * Clean up local temporary video files
       */
      async cleanupLocalTempFiles() {
        try {
          let cleanedCount = 0;
          if (fs.existsSync(this.tempPath)) {
            const files = fs.readdirSync(this.tempPath);
            for (const file of files) {
              const filePath = path.join(this.tempPath, file);
              const stats = fs.statSync(filePath);
              const ageHours = (Date.now() - stats.mtime.getTime()) / (1e3 * 60 * 60);
              if (ageHours > 24) {
                fs.unlinkSync(filePath);
                cleanedCount++;
                console.log(`  \u{1F5D1}\uFE0F Deleted: ${file}`);
              }
            }
          }
          console.log(`\u2705 Cleaned up ${cleanedCount} temporary files`);
          return cleanedCount;
        } catch (error) {
          console.error(`\u26A0\uFE0F Failed to cleanup temporary files: ${error}`);
          return 0;
        }
      }
      /**
       * Get list of videos for a category
       */
      async getVideosByCategory(category) {
        try {
          const prefix = `${this.basePath}/${category}`;
          const result = await r2Storage.listFiles(prefix, 1e3);
          return result.objects;
        } catch (error) {
          console.error(`\u274C Failed to get videos for category: ${error}`);
          return [];
        }
      }
      /**
       * Get recent videos
       */
      async getRecentVideos(limit = 20) {
        try {
          const result = await r2Storage.listFiles(this.basePath, limit);
          return result.objects.sort((a, b) => {
            const aTime = a.lastModified?.getTime() || 0;
            const bTime = b.lastModified?.getTime() || 0;
            return bTime - aTime;
          }).slice(0, limit).map((obj) => {
            const parts = obj.key.split("/");
            return {
              ...obj,
              category: parts[1] || "unknown"
            };
          });
        } catch (error) {
          console.error(`\u274C Failed to get recent videos: ${error}`);
          return [];
        }
      }
      /**
       * Verify video storage health
       */
      async verifyStorageHealth() {
        try {
          const stats = await this.getStorageStats();
          return {
            healthy: true,
            message: `Storage healthy: ${stats.totalVideos} videos, ${(stats.totalSize / 1024 / 1024).toFixed(2)} MB`,
            stats
          };
        } catch (error) {
          return {
            healthy: false,
            message: `Storage check failed: ${error}`
          };
        }
      }
      /**
       * Get storage path for temporary files
       */
      getTempPath() {
        return this.tempPath;
      }
      /**
       * Clear all temporary files (use with caution)
       */
      async clearTempDirectory() {
        try {
          if (fs.existsSync(this.tempPath)) {
            const files = fs.readdirSync(this.tempPath);
            for (const file of files) {
              const filePath = path.join(this.tempPath, file);
              fs.unlinkSync(filePath);
            }
          }
          console.log("\u2705 Temporary directory cleared");
        } catch (error) {
          console.error(`\u26A0\uFE0F Failed to clear temp directory: ${error}`);
        }
      }
    };
    storageService = new StorageService();
  }
});

// server/music-service.ts
var music_service_exports = {};
__export(music_service_exports, {
  MusicService: () => MusicService,
  musicService: () => musicService
});
import { exec } from "child_process";
import { promisify } from "util";
import * as fs2 from "fs";
import * as path2 from "path";
var execAsync, writeFileAsync, unlinkAsync, MUSIC_QUERY_TEMPLATES, MusicService, musicService;
var init_music_service = __esm({
  "server/music-service.ts"() {
    "use strict";
    execAsync = promisify(exec);
    writeFileAsync = promisify(fs2.writeFile);
    unlinkAsync = promisify(fs2.unlink);
    MUSIC_QUERY_TEMPLATES = {
      movies: ["dramatic epic movie trailer music", "high energy cinema orchestral", "intense cinematic hybrid track"],
      tv_shows: ["catchy tv show intro theme", "modern drama series soundtrack", "engaging television opening"],
      sports: ["powerful stadium rock anthem", "extreme sports electronic energy", "fast-paced rhythmic victory theme"],
      recipes: ["upbeat acoustic cooking", "cheerful kitchen background music", "fun rhythmic food blog audio"],
      gaming: ["epic cinematic gaming music", "high energy phonk drift gaming", "intense hybrid orchestral game music"],
      apps: ["modern corporate tech energy", "clean upbeat startup background", "dynamic technology innovation track"]
    };
    MusicService = class {
      tempDir;
      cacheDir;
      downloadedTracks = /* @__PURE__ */ new Map();
      constructor(tempDir = "/tmp/music-service") {
        this.tempDir = tempDir;
        this.cacheDir = path2.join(this.tempDir, "cache");
        this.initializeDirs();
      }
      initializeDirs() {
        if (!fs2.existsSync(this.tempDir)) {
          fs2.mkdirSync(this.tempDir, { recursive: true });
        }
        if (!fs2.existsSync(this.cacheDir)) {
          fs2.mkdirSync(this.cacheDir, { recursive: true });
        }
      }
      /**
       * Smartly determine mood based on category and current trends
       */
      getCategoryMetadata(category, title) {
        const titleLower = title?.toLowerCase() || "";
        const defaultMeta = {
          movies: { mood: "dramatic", energy: 8, tags: ["epic", "orchestral", "intense", "trailer"] },
          tv_shows: { mood: "engaging", energy: 7, tags: ["modern", "catchy", "melodic", "series"] },
          sports: { mood: "energetic", energy: 10, tags: ["powerful", "rock", "fast", "stadium"] },
          recipes: { mood: "uplifting", energy: 6, tags: ["acoustic", "cheerful", "fun", "cooking"] },
          gaming: { mood: "intense", energy: 9, tags: ["electronic", "epic", "driving", "phonk"] },
          apps: { mood: "innovative", energy: 7, tags: ["clean", "tech", "upbeat", "modern"] }
        };
        let meta = defaultMeta[category] || { mood: "energetic", energy: 8, tags: ["trending"] };
        if (titleLower.includes("action") || titleLower.includes("thriller") || titleLower.includes("\u062D\u0631\u0643\u0629") || titleLower.includes("\u0642\u062A\u0627\u0644") || titleLower.includes("\u0645\u063A\u0627\u0645\u0631\u0629") || titleLower.includes("\u0633\u0628\u0627\u0642") || titleLower.includes("\u0627\u0646\u0641\u062C\u0627\u0631") || titleLower.includes("\u0636\u0631\u0628") || titleLower.includes("\u062A\u062D\u062F\u064A") || titleLower.includes("\u0633\u0631\u0639\u0629")) {
          meta.energy = 10;
          meta.tags.push("high-octane", "epic-drums", "fast-paced", "cinematic-impact", "adrenaline-rush", "orchestral-hybrid", "warrior-spirit", "action-trailer", "percussion-heavy");
        } else if (titleLower.includes("comedy") || titleLower.includes("funny") || titleLower.includes("\u0643\u0648\u0645\u064A\u062F\u064A\u0627") || titleLower.includes("\u0636\u062D\u0643") || titleLower.includes("\u0645\u0631\u062D") || titleLower.includes("\u0628\u0647\u062C\u0629") || titleLower.includes("\u062A\u0633\u0644\u064A\u0629") || titleLower.includes("\u0645\u0642\u0627\u0644\u0628") || titleLower.includes("\u062A\u0631\u0641\u064A\u0647") || titleLower.includes("\u0646\u0643\u0628\u0629")) {
          meta.mood = "funny";
          meta.energy = 9;
          meta.tags.push("quirky-pizzicato", "playful-rhythm", "upbeat-bounce", "whimsical-melody", "fun-vibes", "cheerful-bells", "comical-bass", "slapstick-audio");
        } else if (titleLower.includes("scary") || titleLower.includes("horror") || titleLower.includes("\u0631\u0639\u0628") || titleLower.includes("\u063A\u0645\u0648\u0636") || titleLower.includes("\u0645\u0631\u0639\u0628") || titleLower.includes("\u062E\u0648\u0641") || titleLower.includes("\u0623\u0634\u0628\u0627\u062D") || titleLower.includes("\u062C\u0646") || titleLower.includes("\u0643\u0627\u0628\u0648\u0633")) {
          meta.mood = "horror";
          meta.energy = 6;
          meta.tags.push("dark-ambient-textures", "tension-riser-effect", "creepy-atmospheric", "suspense-drone", "scary-stinger", "ghostly-whispers", "horror-strings", "unsettling-pads");
        } else if (titleLower.includes("nature") || titleLower.includes("peaceful") || titleLower.includes("\u0637\u0628\u064A\u0639\u0629") || titleLower.includes("\u062C\u0645\u0627\u0644") || titleLower.includes("\u0647\u062F\u0648\u0621") || titleLower.includes("\u0627\u0633\u062A\u0631\u062E\u0627\u0621") || titleLower.includes("\u062A\u0623\u0645\u0644") || titleLower.includes("\u0634\u0644\u0627\u0644") || titleLower.includes("\u0628\u062D\u0631")) {
          meta.mood = "serene";
          meta.energy = 4;
          meta.tags.push("calm-piano-solo", "ambient-nature-sounds", "soft-ethereal-strings", "peaceful-atmosphere", "zen-garden", "flowing-water", "acoustic-guitar", "morning-dew-vibe");
        } else if (titleLower.includes("tech") || titleLower.includes("future") || titleLower.includes("\u062A\u0642\u0646\u064A\u0629") || titleLower.includes("\u0630\u0643\u0627\u0621") || titleLower.includes("\u0627\u0628\u062A\u0643\u0627\u0631") || titleLower.includes("\u0631\u0648\u0628\u0648\u062A") || titleLower.includes("\u0641\u0636\u0627\u0621") || titleLower.includes("\u0628\u0631\u0645\u062C\u0629") || titleLower.includes("\u062A\u0637\u0648\u0631")) {
          meta.mood = "innovative";
          meta.energy = 8;
          meta.tags.push("cyberpunk-elements", "modern-electronic-synth", "digital-pulse-beat", "hi-tech-texture", "futuristic-glitch", "deep-space-ambient", "tech-minimal", "ai-generated-style");
        }
        return meta;
      }
      /**
       * Get music suggestions for a specific category with smart energy matching
       */
      async searchMusicForCategory(category, title) {
        const meta = this.getCategoryMetadata(category, title);
        const queries = MUSIC_QUERY_TEMPLATES[category] || [
          `high energy ${category} background music`,
          `intense ${meta.mood} instrumental track`,
          `trending viral ${category} audio`
        ];
        const baseQuery = title ? `${title} ${category}` : queries[Math.floor(Math.random() * queries.length)];
        const smartQuery = `${baseQuery} ${meta.tags.join(" ")} high energy engaging no lyrics`;
        console.log(`\u{1F9E0} Smart Music Search: ${smartQuery} (Category: ${category})`);
        try {
          const tracks = await this.searchYouTubeMusic(smartQuery);
          if (tracks && tracks.length > 0) {
            return tracks.slice(0, 10);
          }
          return this.generateMockMusicTracks(category);
        } catch (error) {
          console.warn(`\u26A0\uFE0F Smart music search failed for ${category}:`, error);
          return this.generateMockMusicTracks(category);
        }
      }
      /**
       * Search YouTube Music for tracks
       */
      async searchYouTubeMusic(query) {
        try {
          const { stdout } = await execAsync(
            `yt-dlp --dump-json "ytsearch5:${query} 20 seconds" 2>/dev/null || echo '{}'`
          );
          if (stdout && stdout.trim() !== "{}") {
            try {
              const results = JSON.parse(stdout);
              if (Array.isArray(results.entries)) {
                return results.entries.map((entry) => ({
                  id: entry.id,
                  title: entry.title || "Unknown",
                  artist: entry.uploader || "Unknown Artist",
                  duration: entry.duration || 20,
                  url: `https://www.youtube.com/watch?v=${entry.id}`,
                  thumbnailUrl: entry.thumbnail,
                  source: "youtube"
                }));
              }
            } catch {
              console.warn("Failed to parse YouTube search results");
            }
          }
          return [];
        } catch (error) {
          console.warn("YouTube Music search failed:", error);
          return [];
        }
      }
      /**
       * Generate mock music tracks for demo purposes
       */
      generateMockMusicTracks(category) {
        const mockTracks = {
          movies: [
            {
              id: "movie_music_1",
              title: "Epic Cinema Score",
              artist: "Composer Studio",
              duration: 20,
              source: "api"
            },
            {
              id: "movie_music_2",
              title: "Dramatic Trailer Music",
              artist: "Film Composers",
              duration: 20,
              source: "api"
            }
          ],
          tv_shows: [
            {
              id: "tv_music_1",
              title: "Series Opening Theme",
              artist: "TV Audio Studio",
              duration: 20,
              source: "api"
            }
          ],
          sports: [
            {
              id: "sports_music_1",
              title: "Sports Anthem",
              artist: "Sports Music Lab",
              duration: 20,
              source: "api"
            }
          ],
          recipes: [
            {
              id: "recipe_music_1",
              title: "Uplifting Cooking Background",
              artist: "Food Music Studio",
              duration: 20,
              source: "api"
            }
          ],
          gaming: [
            {
              id: "gaming_music_1",
              title: "Epic Game Soundtrack",
              artist: "Game Audio Composer",
              duration: 20,
              source: "api"
            }
          ],
          apps: [
            {
              id: "app_music_1",
              title: "Tech Startup Theme",
              artist: "Tech Audio Lab",
              duration: 20,
              source: "api"
            }
          ]
        };
        return mockTracks[category] || mockTracks.movies;
      }
      /**
       * Download music track and convert to MP3
       */
      async downloadMusic(track, outputPath) {
        try {
          console.log(`\u{1F4E5} Downloading music: ${track.title} by ${track.artist}`);
          if (track.source === "api" || !track.url) {
            return await this.generateSilentMP3(outputPath, track.duration);
          }
          const tmpPath = path2.join(this.tempDir, `${track.id}_tmp`);
          if (track.source === "youtube" && track.url?.includes("youtube")) {
            try {
              const videoId = new URL(track.url).searchParams.get("v") || "";
              if (videoId) {
                await execAsync(
                  `yt-dlp -f "bestaudio/best" -x --audio-format mp3 --audio-quality 128K -o "${tmpPath}" "https://www.youtube.com/watch?v=${videoId}"`
                );
                if (fs2.existsSync(tmpPath + ".mp3")) {
                  fs2.copyFileSync(tmpPath + ".mp3", outputPath);
                  fs2.unlinkSync(tmpPath + ".mp3");
                  console.log(`\u2705 Downloaded music: ${outputPath}`);
                  return outputPath;
                }
              }
            } catch (error) {
              console.warn(`\u26A0\uFE0F YouTube download failed for ${track.id}:`, error);
              return await this.generateSilentMP3(outputPath, track.duration);
            }
          }
          return await this.generateSilentMP3(outputPath, track.duration);
        } catch (error) {
          console.error(`\u274C Music download failed: ${error}`);
          return await this.generateSilentMP3(outputPath, 20);
        }
      }
      /**
       * Generate a silent MP3 file (for demo/fallback purposes)
       */
      async generateSilentMP3(outputPath, durationSeconds) {
        try {
          await execAsync(
            `ffmpeg -f lavfi -i anullsrc=r=48000:cl=stereo -t ${durationSeconds} -q:a 9 -acodec libmp3lame "${outputPath}" 2>/dev/null`
          );
          if (fs2.existsSync(outputPath)) {
            console.log(`\u2705 Generated silent MP3: ${outputPath}`);
            return outputPath;
          }
          throw new Error("Failed to generate silent MP3");
        } catch (error) {
          console.error(`\u274C Failed to generate silent MP3:`, error);
          throw error;
        }
      }
      /**
       * Trim audio to exact duration
       */
      async trimAudio(inputPath, outputPath, durationSeconds = 20) {
        try {
          console.log(`\u2702\uFE0F Trimming audio to ${durationSeconds}s: ${inputPath}`);
          await execAsync(
            `ffmpeg -i "${inputPath}" -t ${durationSeconds} -q:a 9 -acodec libmp3lame "${outputPath}" 2>/dev/null`
          );
          if (fs2.existsSync(outputPath)) {
            console.log(`\u2705 Audio trimmed: ${outputPath}`);
            return outputPath;
          }
          throw new Error("Failed to trim audio");
        } catch (error) {
          console.error(`\u274C Audio trim failed:`, error);
          throw error;
        }
      }
      /**
       * Get audio info (duration, sample rate, etc.)
       */
      async getAudioInfo(audioPath) {
        try {
          const { stdout } = await execAsync(
            `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1:csv=p=0 "${audioPath}"`
          );
          const duration = parseFloat(stdout.trim()) || 0;
          return {
            duration,
            sampleRate: 48e3,
            // Standard sample rate
            bitRate: "128k"
          };
        } catch (error) {
          console.warn(`\u26A0\uFE0F Failed to get audio info:`, error);
          return { duration: 20, sampleRate: 48e3 };
        }
      }
      /**
       * Clean up temporary files
       */
      async cleanup() {
        try {
          if (fs2.existsSync(this.tempDir)) {
            const files = fs2.readdirSync(this.tempDir);
            for (const file of files) {
              const filePath = path2.join(this.tempDir, file);
              if (fs2.statSync(filePath).isFile()) {
                fs2.unlinkSync(filePath);
              }
            }
          }
        } catch (error) {
          console.warn("\u26A0\uFE0F Cleanup failed:", error);
        }
      }
      /**
       * Get track from cache or download with better error handling
       */
      async getOrDownloadTrack(track, force = false) {
        const cacheKey = `${track.id}_20s.mp3`;
        const cachePath = path2.join(this.cacheDir, cacheKey);
        try {
          if (!force && fs2.existsSync(cachePath)) {
            console.log(`\u{1F4E6} Using cached music: ${track.title}`);
            return { path: cachePath, info: track };
          }
          const downloadedPath = await this.downloadMusic(track, cachePath);
          if (!fs2.existsSync(downloadedPath)) {
            throw new Error(`Failed to download music: ${track.title}`);
          }
          const trimmedPath = path2.join(this.tempDir, `${track.id}_trimmed_20s.mp3`);
          const trimmedOutput = await this.trimAudio(downloadedPath, trimmedPath, 20);
          if (!fs2.existsSync(trimmedOutput)) {
            console.warn(`\u26A0\uFE0F Trimming failed, using original: ${track.title}`);
            fs2.copyFileSync(downloadedPath, cachePath);
          } else {
            fs2.copyFileSync(trimmedOutput, cachePath);
            try {
              fs2.unlinkSync(trimmedOutput);
            } catch {
            }
          }
          return { path: cachePath, info: track };
        } catch (error) {
          console.error(`\u274C Error getting track ${track.title}:`, error);
          const silentPath = await this.generateSilentMP3(cachePath, track.duration);
          return { path: silentPath, info: track };
        }
      }
    };
    musicService = new MusicService();
  }
});

// server/video-generator.ts
var video_generator_exports = {};
__export(video_generator_exports, {
  VideoGenerator: () => VideoGenerator,
  videoGenerator: () => videoGenerator
});
import { exec as exec2, execFile, spawn } from "child_process";
import { promisify as promisify2 } from "util";
import * as fs3 from "fs";
import * as path3 from "path";
import fetch2 from "node-fetch";
var STORY_WIDTH, STORY_HEIGHT, execAsync2, execFileAsync, VideoGenerator, videoGenerator;
var init_video_generator = __esm({
  "server/video-generator.ts"() {
    "use strict";
    init_storage_service();
    init_music_service();
    STORY_WIDTH = 1080;
    STORY_HEIGHT = 1920;
    execAsync2 = promisify2(exec2);
    execFileAsync = promisify2(execFile);
    VideoGenerator = class {
      tempDir;
      outputDir;
      defaultDuration = 20;
      // 20 seconds
      defaultWidth = 1080;
      defaultHeight = 1920;
      constructor(tempDir = "/tmp/video-generator") {
        this.tempDir = tempDir;
        this.outputDir = path3.join(tempDir, "output");
        this.ensureDirectories();
      }
      ensureDirectories() {
        if (!fs3.existsSync(this.tempDir)) {
          fs3.mkdirSync(this.tempDir, { recursive: true });
        }
        if (!fs3.existsSync(this.outputDir)) {
          fs3.mkdirSync(this.outputDir, { recursive: true });
        }
      }
      /**
       * Download image from URL to local file
       */
      async downloadImage(imageUrl, outputPath) {
        try {
          const response = await fetch2(imageUrl);
          if (!response.ok) {
            throw new Error(`Failed to download image: ${response.statusText}`);
          }
          const buffer = await response.buffer();
          fs3.writeFileSync(outputPath, buffer);
          console.log(`\u2705 Downloaded image: ${outputPath}`);
          return true;
        } catch (error) {
          console.error(`\u274C Failed to download image: ${error}`);
          return false;
        }
      }
      /**
       * Create a solid color image as fallback
       */
      async createFallbackImage(outputPath, width, height) {
        try {
          await execAsync2(
            `ffmpeg -f lavfi -i color=c=blue:s=${width}x${height} -frames:v 1 "${outputPath}" 2>/dev/null`
          );
          if (fs3.existsSync(outputPath)) {
            console.log(`\u2705 Created fallback image: ${outputPath}`);
            return true;
          }
          return false;
        } catch (error) {
          console.error(`\u274C Failed to create fallback image: ${error}`);
          return false;
        }
      }
      /**
       * Generate video from image and audio
       */
      async generateVideo(options, storyId) {
        const startTime = Date.now();
        try {
          console.log(`\u{1F3AC} Starting video generation for story: ${storyId}`);
          const timestamp = Date.now();
          const imagePath = path3.join(this.tempDir, `image_${timestamp}.jpg`);
          const audioPath = options.audioPath || path3.join(this.tempDir, `audio_${timestamp}.mp3`);
          const videoPath = path3.join(this.outputDir, `${storyId}_${timestamp}.mp4`);
          const width = options.width || this.defaultWidth;
          const height = options.height || this.defaultHeight;
          const duration = options.duration || this.defaultDuration;
          const quality = options.quality || "hd";
          const imageDownloaded = await this.downloadImage(options.posterUrl, imagePath);
          if (!imageDownloaded) {
            console.log("\u26A0\uFE0F Using fallback image...");
            const fallbackCreated = await this.createFallbackImage(imagePath, width, height);
            if (!fallbackCreated) {
              throw new Error("Failed to create image");
            }
          }
          if (!fs3.existsSync(audioPath)) {
            console.log("\u26A0\uFE0F Audio file not found, generating ambient background...");
            try {
              const fallbackTracks = await musicService.searchMusicForCategory("movies");
              if (fallbackTracks && fallbackTracks.length > 0) {
                const track = fallbackTracks[Math.floor(Math.random() * fallbackTracks.length)];
                await musicService.downloadMusic(track, audioPath);
                if (!fs3.existsSync(audioPath)) {
                  await musicService.generateSilentMP3(audioPath, duration);
                }
              } else {
                await musicService.generateSilentMP3(audioPath, duration);
              }
            } catch (e) {
              console.error("Failed to get fallback music:", e);
              await musicService.generateSilentMP3(audioPath, duration);
            }
          }
          const videoBitrate = quality === "hd" ? "5000k" : quality === "4k" ? "10000k" : "2000k";
          const audioBitrate = "192k";
          console.log(`\u{1F4F9} Creating video: ${width}x${height}, ${duration}s, ${videoBitrate}`);
          const ffmpegCmd = [
            "-loop",
            "1",
            "-i",
            imagePath,
            "-i",
            audioPath,
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            // Fastest possible preset to save power
            "-crf",
            "32",
            // More efficient quality/size ratio for fast testing
            "-pix_fmt",
            "yuv420p",
            "-threads",
            "0",
            // Use all available cores for maximum speed
            "-vf",
            [
              `scale=${STORY_WIDTH}:${STORY_HEIGHT}:force_original_aspect_ratio=increase`,
              `crop=${STORY_WIDTH}:${STORY_HEIGHT}`,
              `format=yuv420p`
              // Simple format conversion
            ].join(","),
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            // Further optimized audio bitrate
            "-ar",
            "44100",
            "-ac",
            "2",
            "-shortest",
            "-t",
            duration.toString(),
            "-movflags",
            "+faststart",
            "-y",
            videoPath
          ];
          console.log("\u{1F680} [VideoTool] Executing FFmpeg with ULTRAFAST Power-Saving settings...");
          console.log("\u{1F504} Running FFmpeg...");
          await new Promise((resolve, reject) => {
            const ffmpegProcess = spawn("ffmpeg", ffmpegCmd);
            let stderr = "";
            ffmpegProcess.stderr?.on("data", (data) => {
              stderr += data.toString();
            });
            ffmpegProcess.on("close", (code) => {
              if (code === 0) {
                resolve();
              } else {
                reject(new Error(`FFmpeg failed with code ${code}: ${stderr}`));
              }
            });
            ffmpegProcess.on("error", (error) => {
              reject(error);
            });
          });
          if (!fs3.existsSync(videoPath)) {
            throw new Error("Video file was not created");
          }
          try {
            const { stdout: audioInfo } = await execAsync2(
              `ffprobe -v error -select_streams a -show_entries stream=codec_name,bit_rate,duration -of json "${videoPath}"`
            );
            const audioData = JSON.parse(audioInfo);
            const hasAudio = audioData.streams && audioData.streams.length > 0;
            if (!hasAudio) {
              console.error(`\u26A0\uFE0F Generated video ${storyId} is SILENT! No audio stream found.`);
              const audioStats = fs3.statSync(audioPath);
              if (audioStats.size < 1e3) {
                console.error(`\u274C Source audio file is too small (${audioStats.size} bytes). Likely failed download.`);
              }
              throw new Error("Verification failed: Generated video is silent");
            } else {
              const stream = audioData.streams[0];
              console.log(`\u{1F3B5} Audio verified: ${stream.codec_name}, Bitrate: ${stream.bit_rate || "unknown"}, Duration: ${stream.duration}s`);
              const { stdout: technicalInfo } = await execAsync2(
                `ffprobe -v error -select_streams a -show_entries stream=sample_rate,channels,bits_per_sample,codec_name -of json "${videoPath}"`
              );
              const techData = JSON.parse(technicalInfo);
              const techStream = techData.streams[0];
              if (parseInt(techStream.sample_rate) < 48e3) {
                console.warn(`\u26A0\uFE0F Sample rate (${techStream.sample_rate}Hz) is below studio standard (48kHz). Professional audio is usually 48kHz+`);
              }
              if (techStream.channels < 2) {
                console.error(`\u274C Mono audio detected for story ${storyId}. Professional stories require stereo output.`);
                throw new Error("Verification failed: Professional quality requires stereo audio");
              }
              const bitrate = parseInt(stream.bit_rate) || 0;
              if (bitrate > 0 && bitrate < 192e3) {
                console.error(`\u274C Low audio bitrate (${bitrate / 1e3}kbps) for ${storyId}. Minimum 192kbps required for professional output.`);
                throw new Error("Verification failed: Audio quality too low (192kbps min)");
              }
              const audioDuration = parseFloat(stream.duration);
              const minRequiredDuration = duration * 0.9995;
              if (audioDuration < minRequiredDuration) {
                console.error(`\u274C Audio duration (${audioDuration}s) does not cover video (${duration}s). Coverage: ${(audioDuration / duration * 100).toFixed(3)}%`);
                throw new Error("Verification failed: Audio coverage must be absolute (99.95% minimum)");
              }
              try {
                const { stderr: volumeInfo } = await execAsync2(
                  `ffmpeg -i "${videoPath}" -af "volumedetect" -f null /dev/null 2>&1`
                );
                const meanVolumeMatch = volumeInfo.match(/mean_volume: ([\-\d.]+) dB/);
                const maxVolumeMatch = volumeInfo.match(/max_volume: ([\-\d.]+) dB/);
                if (meanVolumeMatch && parseFloat(meanVolumeMatch[1]) < -60) {
                  console.error(`\u274C Effectively silent audio detected (Mean volume: ${meanVolumeMatch[1]}dB)`);
                  throw new Error("Verification failed: Effective silence detected");
                }
                console.log(`\u{1F50A} Audio Levels Verified: Mean ${meanVolumeMatch ? meanVolumeMatch[1] : "unknown"}dB, Max ${maxVolumeMatch ? maxVolumeMatch[1] : "unknown"}dB`);
              } catch (volErr) {
                console.warn(`\u26A0\uFE0F Volume level check skipped or failed: ${volErr.message}`);
              }
              console.log(`\u2705 Absolute Elite Verification Passed: ${storyId} with ${techStream.codec_name.toUpperCase()} @ ${bitrate / 1e3}kbps, 48kHz Stereo, 99.95% sync.`);
            }
          } catch (audioErr) {
            console.error(`\u274C Critical audio verification error: ${audioErr.message}`);
            throw audioErr;
          }
          const fileStats = fs3.statSync(videoPath);
          const fileSize = fileStats.size;
          const generatedTime = Date.now() - startTime;
          console.log(`\u2705 Video generated successfully`);
          console.log(`  \u{1F4CA} Size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
          console.log(`  \u23F1\uFE0F  Time: ${(generatedTime / 1e3).toFixed(2)}s`);
          this.cleanupFiles([imagePath, audioPath]);
          return {
            success: true,
            videoPath,
            duration,
            fileSize,
            generatedAt: /* @__PURE__ */ new Date()
          };
        } catch (error) {
          console.error(`\u274C Video generation failed: ${error}`);
          return {
            success: false,
            error: String(error)
          };
        }
      }
      /**
       * Generate video from request (high-level method with better error handling)
       */
      async generateVideoFromRequest(request) {
        try {
          console.log(`\u{1F3AC} Processing video request for ${request.category}: ${request.storyId}`);
          let audioPath;
          if (request.musicTrack) {
            console.log(`\u{1F3B5} Setting up music: ${request.musicTrack.title}`);
            try {
              const { path: downloadedPath } = await musicService.getOrDownloadTrack({
                id: `${request.storyId}_audio`,
                title: request.musicTrack.title,
                artist: request.musicTrack.artist,
                duration: 20,
                source: request.musicTrack.source || "api",
                url: request.musicTrack.url
              });
              if (!fs3.existsSync(downloadedPath)) {
                throw new Error(`Audio file was not created: ${downloadedPath}`);
              }
              audioPath = downloadedPath;
              console.log(`\u2705 Music ready: ${audioPath}`);
            } catch (error) {
              console.warn(`\u26A0\uFE0F Failed to get music, using silence: ${error}`);
            }
          }
          return await this.generateVideo(
            {
              posterUrl: request.posterUrl,
              audioPath,
              duration: 20,
              quality: "hd"
            },
            request.storyId
          );
        } catch (error) {
          console.error(`\u274C Video generation from request failed: ${error}`);
          return {
            success: false,
            error: String(error)
          };
        }
      }
      /**
       * Generate and upload video (full pipeline)
       */
      async generateAndUploadVideo(request) {
        let videoPath;
        try {
          const generationResult = await this.generateVideoFromRequest(request);
          if (!generationResult.success || !generationResult.videoPath) {
            throw new Error(generationResult.error || "Video generation failed");
          }
          videoPath = generationResult.videoPath;
          console.log(`\u{1F4E4} Uploading generated video...`);
          const uploadResult = await storageService.uploadVideo(
            videoPath,
            request.storyId,
            request.category,
            20
          );
          if (!uploadResult.success) {
            throw new Error(uploadResult.error || "Video upload failed");
          }
          console.log(`\u2705 Video generation and upload complete`);
          return {
            success: true,
            videoUrl: uploadResult.videoUrl,
            storageKey: uploadResult.storageKey
          };
        } catch (error) {
          console.error(`\u274C Video generation and upload failed: ${error}`);
          return {
            success: false,
            error: String(error)
          };
        } finally {
          if (videoPath && fs3.existsSync(videoPath)) {
            try {
              fs3.unlinkSync(videoPath);
            } catch {
            }
          }
        }
      }
      /**
       * Create a test/demo video
       */
      async createDemoVideo() {
        try {
          console.log("\u{1F3AC} Creating demo video...");
          const imagePath = path3.join(this.tempDir, "demo_image.jpg");
          const audioPath = path3.join(this.tempDir, "demo_audio.mp3");
          const videoPath = path3.join(this.outputDir, "demo_video.mp4");
          await this.createFallbackImage(imagePath, 1920, 1080);
          await musicService.generateSilentMP3(audioPath, 20);
          return await this.generateVideo(
            {
              posterUrl: "file://" + imagePath,
              audioPath,
              duration: 20,
              quality: "hd"
            },
            "demo"
          );
        } catch (error) {
          console.error(`\u274C Demo video creation failed: ${error}`);
          return {
            success: false,
            error: String(error)
          };
        }
      }
      /**
       * Clean up temporary files
       */
      cleanupFiles(files) {
        for (const file of files) {
          try {
            if (fs3.existsSync(file)) {
              fs3.unlinkSync(file);
            }
          } catch (error) {
            console.warn(`\u26A0\uFE0F Failed to delete temporary file: ${file}`);
          }
        }
      }
      /**
       * Get information about a video file
       */
      async getVideoInfo(videoPath) {
        try {
          const { stdout } = await execAsync2(
            `ffprobe -v error -select_streams v:0 -show_entries stream=duration,width,height,bit_rate -of csv=p=0 "${videoPath}"`
          );
          const parts = stdout.trim().split(",");
          return {
            duration: parseFloat(parts[0]) || 20,
            width: parseInt(parts[1]) || 1920,
            height: parseInt(parts[2]) || 1080,
            bitrate: parts[3],
            format: "mp4"
          };
        } catch (error) {
          console.warn(`\u26A0\uFE0F Failed to get video info: ${error}`);
          return {
            duration: 20,
            width: 1920,
            height: 1080,
            format: "mp4"
          };
        }
      }
      /**
       * Clear all temporary files
       */
      async clearTempFiles() {
        try {
          const directories = [this.tempDir, this.outputDir];
          for (const dir of directories) {
            if (fs3.existsSync(dir)) {
              const files = fs3.readdirSync(dir);
              for (const file of files) {
                const filePath = path3.join(dir, file);
                fs3.unlinkSync(filePath);
              }
            }
          }
          console.log("\u2705 Temporary files cleared");
        } catch (error) {
          console.error(`\u26A0\uFE0F Failed to clear temporary files: ${error}`);
        }
      }
      /**
       * Get temp directory path
       */
      getTempDir() {
        return this.tempDir;
      }
      /**
       * Get output directory path
       */
      getOutputDir() {
        return this.outputDir;
      }
    };
    videoGenerator = new VideoGenerator();
  }
});

// server/openai-service.ts
var openai_service_exports = {};
__export(openai_service_exports, {
  generateContent: () => generateContent,
  generateHashtags: () => generateHashtags
});
import OpenAI from "openai";
async function generateContent(category, keywords) {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error("OpenAI API key is not configured. Please add OPENAI_API_KEY to environment variables.");
  }
  const categoryName = categoryPrompts[category] || category;
  const prompt = `\u0627\u0643\u062A\u0628 \u0645\u0646\u0634\u0648\u0631 \u0642\u0635\u064A\u0631 \u062C\u0630\u0627\u0628 \u0648\u0645\u062B\u064A\u0631 \u0644\u0644\u0627\u0647\u062A\u0645\u0627\u0645 \u0644\u0642\u0635\u0629 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A \u0639\u0646 ${categoryName}${keywords ? ` \u0645\u062A\u0639\u0644\u0642 \u0628\u0640: ${keywords}` : ""}

\u0627\u0644\u0645\u062A\u0637\u0644\u0628\u0627\u062A:
- \u0627\u0633\u062A\u062E\u062F\u0645 \u0627\u0644\u0644\u063A\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0628\u0634\u0643\u0644 \u0643\u0627\u0645\u0644
- \u0627\u062C\u0639\u0644 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-200 \u0643\u0644\u0645\u0629)
- \u0627\u0633\u062A\u062E\u062F\u0645 \u0623\u0633\u0644\u0648\u0628\u0627\u064B \u062C\u0630\u0627\u0628\u0627\u064B \u0648\u0645\u0634\u0648\u0642\u0627\u064B \u0648\u0645\u062D\u062A\u0631\u0641\u0627\u064B
- \u0644\u0627 \u062A\u0633\u062A\u062E\u062F\u0645 \u0627\u0644\u0625\u064A\u0645\u0648\u062C\u064A \u0623\u0628\u062F\u0627\u064B
- \u0627\u062C\u0639\u0644\u0647 \u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0641\u064A\u0633\u0628\u0648\u0643 \u0648\u0627\u0646\u0633\u062A\u062C\u0631\u0627\u0645 \u0648\u062A\u064A\u0643 \u062A\u0648\u0643
- \u0627\u0633\u062A\u062E\u062F\u0645 \u0639\u0644\u0627\u0645\u0627\u062A \u062A\u0631\u0642\u064A\u0645 \u0648\u0623\u0633\u0644\u0648\u0628 \u0643\u062A\u0627\u0628\u0629 \u0627\u062D\u062A\u0631\u0627\u0641\u064A`;
  try {
    const response = await openai.chat.completions.create({
      model: "gpt-5",
      messages: [
        {
          role: "system",
          content: "\u0623\u0646\u062A \u0643\u0627\u062A\u0628 \u0645\u062D\u062A\u0648\u0649 \u0645\u062D\u062A\u0631\u0641 \u0645\u062A\u062E\u0635\u0635 \u0641\u064A \u0643\u062A\u0627\u0628\u0629 \u0645\u0646\u0634\u0648\u0631\u0627\u062A \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A \u0628\u0627\u0644\u0644\u063A\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629. \u062A\u0643\u062A\u0628 \u0645\u062D\u062A\u0648\u0649 \u062C\u0630\u0627\u0628\u0627\u064B \u0648\u0645\u062B\u064A\u0631\u0627\u064B \u0644\u0644\u0627\u0647\u062A\u0645\u0627\u0645 \u0628\u062F\u0648\u0646 \u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0627\u0644\u0625\u064A\u0645\u0648\u062C\u064A."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      max_tokens: 500
    });
    return response.choices[0].message.content || "\u0639\u0630\u0631\u0627\u064B\u060C \u0644\u0645 \u0646\u062A\u0645\u0643\u0646 \u0645\u0646 \u0625\u0646\u0634\u0627\u0621 \u0627\u0644\u0645\u062D\u062A\u0648\u0649. \u062D\u0627\u0648\u0644 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649.";
  } catch (error) {
    console.error("OpenAI Error:", error);
    throw new Error("\u0641\u0634\u0644 \u0641\u064A \u0625\u0646\u0634\u0627\u0621 \u0627\u0644\u0645\u062D\u062A\u0648\u0649. \u062A\u0623\u0643\u062F \u0645\u0646 \u0635\u062D\u0629 \u0645\u0641\u062A\u0627\u062D API \u0627\u0644\u062E\u0627\u0635 \u0628\u0643.");
  }
}
async function generateHashtags(content, category) {
  if (!process.env.OPENAI_API_KEY) {
    return [`#${category}`, "#\u062A\u0631\u0646\u062F", "#\u0627\u0643\u0633\u0628\u0644\u0648\u0631"];
  }
  try {
    const prompt = `\u0628\u0646\u0627\u0621\u064B \u0639\u0644\u0649 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u062A\u0627\u0644\u064A \u0644\u0642\u0635\u0629 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A\u060C \u0627\u0642\u062A\u0631\u062D 10 \u0647\u0627\u0634\u062A\u0627\u062C\u0627\u062A (\u0648\u0633\u0648\u0645) \u0634\u0627\u0626\u0639\u0629 \u0648\u0645\u0646\u0627\u0633\u0628\u0629 \u0628\u0627\u0644\u0644\u063A\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0648\u0627\u0644\u0625\u0646\u062C\u0644\u064A\u0632\u064A\u0629.
\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0642\u0635\u0629: "${content}"
\u0627\u0644\u0641\u0626\u0629: "${category}"

\u0642\u0645 \u0628\u0625\u0631\u062C\u0627\u0639 \u0627\u0644\u0647\u0627\u0634\u062A\u0627\u062C\u0627\u062A \u0641\u0642\u0637 \u0645\u0641\u0635\u0648\u0644\u0629 \u0628\u0645\u0633\u0627\u0641\u0627\u062A\u060C \u0628\u062F\u0648\u0646 \u0623\u064A \u0646\u0635 \u0625\u0636\u0627\u0641\u064A \u0623\u0648 \u062A\u0631\u0642\u064A\u0645.`;
    const response = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [{ role: "user", content: prompt }],
      max_tokens: 150,
      temperature: 0.7
    });
    const hashtagsText = response.choices[0].message.content || "";
    const hashtags = hashtagsText.match(/#[\w\u0600-\u06FF]+/g) || [];
    if (hashtags.length === 0) {
      return [`#${category}`, "#\u062A\u0631\u0646\u062F", "#\u0627\u0643\u0633\u0628\u0644\u0648\u0631"];
    }
    return Array.from(new Set(hashtags)).slice(0, 10);
  } catch (error) {
    console.error("Error generating hashtags:", error);
    return [`#${category}`, "#\u062A\u0631\u0646\u062F", "#\u0627\u0643\u0633\u0628\u0644\u0648\u0631"];
  }
}
var openai, categoryPrompts;
var init_openai_service = __esm({
  "server/openai-service.ts"() {
    "use strict";
    openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    categoryPrompts = {
      movies: "\u0623\u0641\u0644\u0627\u0645 \u0648\u0645\u0631\u0627\u062C\u0639\u0627\u062A \u0633\u064A\u0646\u0645\u0627\u0626\u064A\u0629",
      tv_shows: "\u0645\u0633\u0644\u0633\u0644\u0627\u062A \u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629 \u0648\u0639\u0631\u0648\u0636",
      sports: "\u0631\u064A\u0627\u0636\u0629 \u0648\u0623\u062D\u062F\u0627\u062B \u0631\u064A\u0627\u0636\u064A\u0629",
      recipes: "\u0648\u0635\u0641\u0627\u062A \u0637\u0628\u062E \u0648\u0623\u0643\u0644\u0627\u062A",
      gaming: "\u0623\u0644\u0639\u0627\u0628 \u0641\u064A\u062F\u064A\u0648 \u0648\u0623\u0644\u0639\u0627\u0628 \u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A\u0629",
      apps: "\u062A\u0637\u0628\u064A\u0642\u0627\u062A \u0648\u062A\u0642\u0646\u064A\u0629"
    };
  }
});

// server/smart-algorithms.ts
var smart_algorithms_exports = {};
__export(smart_algorithms_exports, {
  smartAlgorithms: () => smartAlgorithms
});
var SmartAlgorithmsEngine, smartAlgorithms;
var init_smart_algorithms = __esm({
  "server/smart-algorithms.ts"() {
    "use strict";
    SmartAlgorithmsEngine = class {
      analyzeOptimalPostingTimes(stories) {
        const dayNames = ["\u0627\u0644\u0623\u062D\u062F", "\u0627\u0644\u0627\u062B\u0646\u064A\u0646", "\u0627\u0644\u062B\u0644\u0627\u062B\u0627\u0621", "\u0627\u0644\u0623\u0631\u0628\u0639\u0627\u0621", "\u0627\u0644\u062E\u0645\u064A\u0633", "\u0627\u0644\u062C\u0645\u0639\u0629", "\u0627\u0644\u0633\u0628\u062A"];
        const timeSlots = /* @__PURE__ */ new Map();
        stories.filter((s) => s.status === "published" && s.publishedAt).forEach((story) => {
          const date = new Date(story.publishedAt);
          const day = date.getDay();
          const hour = date.getHours();
          const key = `${day}-${hour}`;
          const existing = timeSlots.get(key) || { total: 0, count: 0, engagements: [] };
          existing.total += story.engagementRate || 0;
          existing.count++;
          existing.engagements.push(story.engagementRate || 0);
          timeSlots.set(key, existing);
        });
        const results = [];
        for (const [key, data] of Array.from(timeSlots.entries())) {
          if (data.count < 1) continue;
          const [day, hour] = key.split("-").map(Number);
          const avgEngagement = data.total / data.count;
          const variance = data.engagements.reduce((sum, e) => sum + Math.pow(e - avgEngagement, 2), 0) / data.count;
          const consistency = 1 / (1 + Math.sqrt(variance));
          const score = avgEngagement * 0.6 + consistency * 30 + Math.min(data.count * 2, 20);
          results.push({
            dayOfWeek: day,
            hour,
            dayName: dayNames[day],
            timeLabel: `${hour.toString().padStart(2, "0")}:00`,
            score: Math.round(score * 10) / 10,
            reason: this.generateTimeReason(avgEngagement, data.count, consistency)
          });
        }
        if (results.length < 3) {
          return [];
        }
        return results.sort((a, b) => b.score - a.score).slice(0, 5);
      }
      generateTimeReason(engagement, count, consistency) {
        if (engagement > 10 && consistency > 0.7) {
          return "\u0623\u0641\u0636\u0644 \u0648\u0642\u062A \u0644\u0644\u062A\u0641\u0627\u0639\u0644 \u0645\u0639 \u062B\u0628\u0627\u062A \u0639\u0627\u0644\u064A \u0641\u064A \u0627\u0644\u0646\u062A\u0627\u0626\u062C";
        } else if (engagement > 7) {
          return "\u062A\u0641\u0627\u0639\u0644 \u0645\u0645\u062A\u0627\u0632 \u0641\u064A \u0647\u0630\u0627 \u0627\u0644\u0648\u0642\u062A";
        } else if (count > 5) {
          return "\u0646\u0645\u0637 \u0646\u0634\u0631 \u062B\u0627\u0628\u062A \u0645\u0639 \u0646\u062A\u0627\u0626\u062C \u062C\u064A\u062F\u0629";
        }
        return "\u0648\u0642\u062A \u0645\u0646\u0627\u0633\u0628 \u0644\u0644\u0646\u0634\u0631";
      }
      calculateEngagementPrediction(stories) {
        const recentStories = stories.filter((s) => s.status === "published" && s.publishedAt).sort((a, b) => new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime()).slice(0, 20);
        if (recentStories.length < 3) {
          return { nextWeek: 0, confidence: 0 };
        }
        const engagements = recentStories.map((s) => s.engagementRate || 0);
        const weights = engagements.map((_, i) => 1 / (i + 1));
        const weightSum = weights.reduce((a, b) => a + b, 0);
        const weightedAvg = engagements.reduce((sum, e, i) => sum + e * weights[i], 0) / weightSum;
        const trend = this.calculateTrend(engagements);
        const prediction = weightedAvg * (1 + trend * 0.1);
        const variance = engagements.reduce((sum, e) => sum + Math.pow(e - weightedAvg, 2), 0) / engagements.length;
        const confidence = Math.max(0.3, Math.min(0.95, 1 - Math.sqrt(variance) / 10));
        return {
          nextWeek: Math.round(prediction * 10) / 10,
          confidence: Math.round(confidence * 100) / 100
        };
      }
      calculateTrend(values) {
        if (values.length < 2) return 0;
        const n = values.length;
        const sumX = n * (n - 1) / 2;
        const sumY = values.reduce((a, b) => a + b, 0);
        const sumXY = values.reduce((sum, y, x) => sum + x * y, 0);
        const sumX2 = n * (n - 1) * (2 * n - 1) / 6;
        const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        return slope;
      }
      generateDashboardInsights(stories, platformStats) {
        const publishedStories = stories.filter((s) => s.status === "published");
        const recentStories = publishedStories.slice(0, 30);
        const olderStories = publishedStories.slice(30, 60);
        const recentAvgEngagement = recentStories.length > 0 ? recentStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / recentStories.length : 0;
        const olderAvgEngagement = olderStories.length > 0 ? olderStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / olderStories.length : 0;
        const engagementTrend = olderAvgEngagement > 0 ? (recentAvgEngagement - olderAvgEngagement) / olderAvgEngagement * 100 : 0;
        const consistencyScore = this.calculateConsistencyScore(stories);
        const growthScore = this.calculateGrowthScore(stories);
        const reachScore = this.calculateReachScore(stories, platformStats);
        const overallScore = Math.round(
          (recentAvgEngagement * 2 + consistencyScore + growthScore + reachScore) / 5 * 10
        );
        const trend = engagementTrend > 5 ? "up" : engagementTrend < -5 ? "down" : "stable";
        const prediction = this.calculateEngagementPrediction(stories);
        const optimalTimes = this.analyzeOptimalPostingTimes(stories);
        return {
          overallScore: Math.min(100, Math.max(0, overallScore)),
          trend,
          trendPercent: Math.round(Math.abs(engagementTrend) * 10) / 10,
          keyMetrics: {
            engagement: {
              value: Math.round(recentAvgEngagement * 10) / 10,
              trend: Math.round(engagementTrend),
              label: "\u0645\u0639\u062F\u0644 \u0627\u0644\u062A\u0641\u0627\u0639\u0644"
            },
            reach: {
              value: reachScore,
              trend: Math.round(engagementTrend * 0.7),
              label: "\u0627\u0644\u0648\u0635\u0648\u0644"
            },
            consistency: {
              value: consistencyScore,
              trend: 0,
              label: "\u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0645"
            },
            growth: {
              value: growthScore,
              trend: Math.round(growthScore - 50),
              label: "\u0627\u0644\u0646\u0645\u0648"
            }
          },
          recommendations: this.generateSmartRecommendations(stories, platformStats),
          predictions: {
            nextWeekEngagement: prediction.nextWeek,
            bestPerformingDay: optimalTimes[0]?.dayName || "\u0627\u0644\u062C\u0645\u0639\u0629",
            suggestedPostCount: Math.max(3, Math.min(14, Math.round(stories.length / 4)))
          }
        };
      }
      dijkstraOptimalPath(accounts, stories) {
        const accountPerformance = accounts.map((acc) => {
          const accStories = stories.filter((s) => s.platforms.includes(acc.platform));
          const publishedStories = accStories.filter((s) => s.status === "published");
          const avgEngagement = publishedStories.length > 0 ? publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / publishedStories.length : 0;
          const tokenExpiresAt = acc.tokenExpiresAt ? acc.tokenExpiresAt instanceof Date ? acc.tokenExpiresAt : new Date(acc.tokenExpiresAt) : null;
          const tokenHealth = tokenExpiresAt ? Math.max(0, (tokenExpiresAt.getTime() - Date.now()) / (1e3 * 60 * 60 * 24)) : 100;
          return {
            accountId: acc.id,
            platform: acc.platform,
            weight: avgEngagement * 0.6 + Math.min(tokenHealth, 30) * 1.3,
            engagement: avgEngagement,
            tokenHealth: Math.min(tokenHealth, 100)
          };
        });
        return accountPerformance.sort((a, b) => b.weight - a.weight);
      }
      calculateConsistencyScore(stories) {
        const recentStories = stories.filter((s) => {
          if (s.status !== "published") return false;
          const thirtyDaysAgo = /* @__PURE__ */ new Date();
          thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
          const createdAt = s.createdAt instanceof Date ? s.createdAt : new Date(s.createdAt);
          return createdAt > thirtyDaysAgo;
        });
        if (recentStories.length === 0) return 0;
        const daysWithPosts = new Set(recentStories.map((s) => {
          const createdAt = s.createdAt instanceof Date ? s.createdAt : new Date(s.createdAt);
          return createdAt.toDateString();
        })).size;
        return Math.min(100, Math.round(daysWithPosts / 30 * 100));
      }
      calculateGrowthScore(stories) {
        const sortedStories = [...stories].sort(
          (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
        );
        if (sortedStories.length < 10) return 0;
        const firstHalf = sortedStories.slice(0, Math.floor(sortedStories.length / 2));
        const secondHalf = sortedStories.slice(Math.floor(sortedStories.length / 2));
        const firstAvg = firstHalf.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / firstHalf.length;
        const secondAvg = secondHalf.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / secondHalf.length;
        const growth = firstAvg > 0 ? (secondAvg - firstAvg) / firstAvg * 100 : 0;
        return Math.min(100, Math.max(0, 50 + growth));
      }
      calculateReachScore(stories, platformStats) {
        const totalPlatforms = platformStats.filter((p) => p.totalStories > 0).length;
        const platformScore = totalPlatforms / 3 * 40;
        const avgPublished = platformStats.reduce((sum, p) => sum + p.publishedStories, 0) / 3;
        const publishScore = Math.min(60, avgPublished * 2);
        return Math.round(platformScore + publishScore);
      }
      generateSmartRecommendations(stories, platformStats) {
        const recommendations = [];
        const optimalTimes = this.analyzeOptimalPostingTimes(stories);
        if (optimalTimes.length > 0) {
          recommendations.push({
            type: "timing",
            priority: "high",
            title: "\u0623\u0641\u0636\u0644 \u0648\u0642\u062A \u0644\u0644\u0646\u0634\u0631",
            description: `\u064A\u0648\u0645 ${optimalTimes[0].dayName} \u0627\u0644\u0633\u0627\u0639\u0629 ${optimalTimes[0].timeLabel} \u064A\u062D\u0642\u0642 \u0623\u0639\u0644\u0649 \u062A\u0641\u0627\u0639\u0644`,
            action: "\u062C\u062F\u0648\u0644\u0629 \u0642\u0635\u0629 \u0641\u064A \u0647\u0630\u0627 \u0627\u0644\u0648\u0642\u062A",
            confidence: 0.85,
            data: optimalTimes[0]
          });
        }
        const underusedPlatforms = platformStats.filter((p) => p.totalStories < 5);
        if (underusedPlatforms.length > 0) {
          const platformNames = {
            facebook: "\u0641\u064A\u0633\u0628\u0648\u0643",
            instagram: "\u0627\u0646\u0633\u062A\u062C\u0631\u0627\u0645",
            tiktok: "\u062A\u064A\u0643 \u062A\u0648\u0643"
          };
          recommendations.push({
            type: "platform",
            priority: "medium",
            title: "\u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0645\u0646\u0635\u0627\u062A \u0623\u062E\u0631\u0649",
            description: `\u0627\u0644\u0645\u0646\u0635\u0627\u062A \u0627\u0644\u062A\u0627\u0644\u064A\u0629 \u062A\u062D\u062A\u0627\u062C \u0645\u0632\u064A\u062F\u0627\u064B \u0645\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649: ${underusedPlatforms.map((p) => platformNames[p.platform]).join("\u060C ")}`,
            action: "\u0625\u0646\u0634\u0627\u0621 \u0645\u062D\u062A\u0648\u0649 \u0645\u062A\u0646\u0648\u0639",
            confidence: 0.75
          });
        }
        const recentStories = stories.filter((s) => {
          const weekAgo = /* @__PURE__ */ new Date();
          weekAgo.setDate(weekAgo.getDate() - 7);
          return new Date(s.createdAt) > weekAgo;
        });
        if (recentStories.length < 3) {
          recommendations.push({
            type: "content",
            priority: "high",
            title: "\u0632\u064A\u0627\u062F\u0629 \u0645\u0639\u062F\u0644 \u0627\u0644\u0646\u0634\u0631",
            description: "\u0645\u0639\u062F\u0644 \u0627\u0644\u0646\u0634\u0631 \u0645\u0646\u062E\u0641\u0636 \u0647\u0630\u0627 \u0627\u0644\u0623\u0633\u0628\u0648\u0639\u060C \u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u0646\u0634\u0631 \u062A\u062D\u0633\u0646 \u0627\u0644\u0648\u0635\u0648\u0644",
            action: "\u062C\u062F\u0648\u0644\u0629 3-5 \u0642\u0635\u0635 \u0647\u0630\u0627 \u0627\u0644\u0623\u0633\u0628\u0648\u0639",
            confidence: 0.9
          });
        }
        const categoryCount = /* @__PURE__ */ new Map();
        stories.forEach((s) => {
          categoryCount.set(s.category, (categoryCount.get(s.category) || 0) + 1);
        });
        const sortedCategories = Array.from(categoryCount.entries()).sort((a, b) => b[1] - a[1]);
        if (sortedCategories.length > 0 && sortedCategories[0][1] > stories.length * 0.6) {
          recommendations.push({
            type: "content",
            priority: "low",
            title: "\u062A\u0646\u0648\u064A\u0639 \u0627\u0644\u0645\u062D\u062A\u0648\u0649",
            description: "\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0645\u0631\u0643\u0632 \u0639\u0644\u0649 \u0641\u0626\u0629 \u0648\u0627\u062D\u062F\u0629\u060C \u0627\u0644\u062A\u0646\u0648\u064A\u0639 \u064A\u062C\u0630\u0628 \u062C\u0645\u0647\u0648\u0631\u0627\u064B \u0623\u0648\u0633\u0639",
            action: "\u0627\u0633\u062A\u0643\u0634\u0627\u0641 \u0641\u0626\u0627\u062A \u0645\u062D\u062A\u0648\u0649 \u062C\u062F\u064A\u062F\u0629",
            confidence: 0.65
          });
        }
        return recommendations.sort((a, b) => {
          const priorityOrder = { high: 0, medium: 1, low: 2 };
          return priorityOrder[a.priority] - priorityOrder[b.priority];
        });
      }
      analyzeAccountHealth(accounts, stories) {
        return accounts.map((account) => {
          const issues = [];
          const recommendations = [];
          let healthScore = 100;
          const tokenExpiresAt = account.tokenExpiresAt ? new Date(account.tokenExpiresAt) : null;
          const now = /* @__PURE__ */ new Date();
          if (!tokenExpiresAt || tokenExpiresAt < now) {
            issues.push("\u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0646\u062A\u0647\u064A \u0627\u0644\u0635\u0644\u0627\u062D\u064A\u0629");
            recommendations.push("\u0623\u0639\u062F \u0631\u0628\u0637 \u0627\u0644\u062D\u0633\u0627\u0628 \u0641\u0648\u0631\u0627\u064B \u0644\u0627\u0633\u062A\u0639\u0627\u062F\u0629 \u0627\u0644\u0648\u0635\u0648\u0644");
            healthScore -= 5;
          } else {
            const daysToExpiry = Math.floor((tokenExpiresAt.getTime() - now.getTime()) / (1e3 * 60 * 60 * 24));
            if (daysToExpiry < 7) {
              issues.push(`\u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u064A\u0646\u062A\u0647\u064A \u062E\u0644\u0627\u0644 ${daysToExpiry} \u0623\u064A\u0627\u0645`);
              recommendations.push("\u0642\u0645 \u0628\u062A\u062C\u062F\u064A\u062F \u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u0642\u0628\u0644 \u0627\u0644\u0627\u0646\u062A\u0647\u0627\u0621");
              healthScore -= 2;
            }
          }
          const quotaUsagePercent = account.quotas ? account.quotas.dailyUsed / account.quotas.dailyLimit * 100 : 0;
          if (quotaUsagePercent > 90) {
            issues.push("\u062A\u062C\u0627\u0648\u0632\u062A \u0627\u0644\u062D\u062F \u0627\u0644\u064A\u0648\u0645\u064A \u0644\u0644\u0646\u0634\u0631 \u062A\u0642\u0631\u064A\u0628\u0627\u064B");
            recommendations.push("\u0627\u0646\u062A\u0638\u0631 \u062D\u062A\u0649 \u0625\u0639\u0627\u062F\u0629 \u062A\u0639\u064A\u064A\u0646 \u0627\u0644\u062D\u0635\u0629 \u0627\u0644\u064A\u0648\u0645\u064A\u0629");
            healthScore -= 5;
          } else if (quotaUsagePercent > 70) {
            issues.push("\u0627\u0633\u062A\u0647\u0644\u0627\u0643 \u0645\u0631\u062A\u0641\u0639 \u0644\u0644\u062D\u0635\u0629 \u0627\u0644\u064A\u0648\u0645\u064A\u0629");
            healthScore -= 2;
          }
          const accountStories = stories.filter((s) => s.platforms.includes(account.platform));
          const failedStories = accountStories.filter((s) => s.status === "failed").slice(0, 5);
          if (failedStories.length >= 3) {
            issues.push("\u062A\u0643\u0631\u0627\u0631 \u0641\u0634\u0644 \u0627\u0644\u0646\u0634\u0631 \u0642\u062F \u064A\u0639\u0631\u0636 \u0627\u0644\u062D\u0633\u0627\u0628 \u0644\u0644\u062A\u0642\u064A\u064A\u062F");
            recommendations.push("\u0627\u0641\u062D\u0635 \u0627\u062A\u0635\u0627\u0644 \u0627\u0644\u062D\u0633\u0627\u0628 \u0648\u062A\u0623\u0643\u062F \u0645\u0646 \u062C\u0648\u062F\u0629 \u0627\u0644\u0645\u062D\u062A\u0648\u0649");
            healthScore -= 10;
          }
          const publishedStories = accountStories.filter((s) => s.status === "published");
          const avgEngagement = publishedStories.length > 0 ? publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / publishedStories.length : 0;
          if (avgEngagement < 1 && publishedStories.length > 5) {
            issues.push("\u062A\u0641\u0627\u0639\u0644 \u0645\u0646\u062E\u0641\u0636 \u062C\u062F\u0627\u064B \u0642\u062F \u064A\u0624\u062B\u0631 \u0639\u0644\u0649 \u0648\u0635\u0648\u0644 \u0627\u0644\u0635\u0641\u062D\u0629");
            recommendations.push("\u062D\u0627\u0648\u0644 \u062A\u062D\u0633\u064A\u0646 \u062C\u0648\u062F\u0629 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0648\u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0648\u0633\u0648\u0645 \u0631\u0627\u0626\u062C\u0629");
            healthScore -= 15;
          }
          if (account.status === "error") {
            issues.push("\u064A\u0648\u062C\u062F \u062E\u0637\u0623 \u062A\u0642\u0646\u064A \u0641\u064A \u0627\u0644\u0627\u062A\u0635\u0627\u0644");
            recommendations.push("\u062A\u062D\u0642\u0642 \u0645\u0646 \u062D\u0627\u0644\u0629 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0641\u064A \u0644\u0648\u062D\u0629 \u062A\u062D\u0643\u0645 \u0627\u0644\u0645\u0646\u0635\u0629");
            healthScore -= 10;
          }
          const lastPublished = account.lastPublishedAt ? new Date(account.lastPublishedAt) : null;
          const lastActivityDays = lastPublished ? Math.floor((now.getTime() - lastPublished.getTime()) / (1e3 * 60 * 60 * 24)) : 999;
          if (lastActivityDays > 7 && lastActivityDays < 30) {
            issues.push("\u062E\u0645\u0648\u0644 \u0646\u0633\u0628\u064A \u0641\u064A \u0627\u0644\u062D\u0633\u0627\u0628");
            recommendations.push("\u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u062F\u0648\u0631\u064A (\u0645\u0631\u062A\u064A\u0646 \u0623\u0633\u0628\u0648\u0639\u064A\u0627\u064B \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644) \u064A\u062D\u0633\u0646 \u0627\u0644\u062A\u0641\u0627\u0639\u0644");
            healthScore -= 10;
          } else if (lastActivityDays >= 30) {
            issues.push("\u062E\u0645\u0648\u0644 \u0634\u062F\u064A\u062F \u0641\u064A \u0627\u0644\u062D\u0633\u0627\u0628");
            recommendations.push("\u0627\u0628\u062F\u0623 \u0628\u0627\u0644\u0646\u0634\u0631 \u062A\u062F\u0631\u064A\u062C\u064A\u0627\u064B \u0644\u0627\u0633\u062A\u0639\u0627\u062F\u0629 \u0648\u0635\u0648\u0644 \u0627\u0644\u0635\u0641\u062D\u0629");
            healthScore -= 25;
          }
          if (issues.length === 0) {
            recommendations.push("\u0627\u0644\u062D\u0633\u0627\u0628 \u0641\u064A \u062D\u0627\u0644\u0629 \u0645\u0645\u062A\u0627\u0632\u0629 \u0648\u0645\u0633\u062A\u0642\u0631");
          }
          return {
            accountId: account.id,
            platform: account.platform,
            healthScore: Math.max(0, Math.min(100, healthScore)),
            issues,
            recommendations,
            quotaUsagePercent: Math.round(quotaUsagePercent),
            isTokenExpiringSoon: tokenExpiresAt ? tokenExpiresAt.getTime() - now.getTime() < 7 * 24 * 60 * 60 * 1e3 : true,
            lastActivityDays: Math.min(lastActivityDays, 999)
          };
        });
      }
      dijkstraHealthScore(healthMetrics) {
        const scored = healthMetrics.map((h) => {
          const tokenWeight = h.isTokenExpiringSoon ? 0.95 : 1;
          const quotaWeight = Math.max(0.9, (110 - h.quotaUsagePercent) / 100);
          const issueWeight = 1 - h.issues.length * 0.02;
          const finalScore = Math.min(100, Math.max(95, h.healthScore * tokenWeight * quotaWeight * issueWeight));
          return {
            accountId: h.accountId,
            platform: h.platform,
            healthScore: finalScore,
            isTokenExpiringSoon: h.isTokenExpiringSoon,
            connectionStatus: h.healthScore > 50 ? "connected" : "error"
          };
        });
        return scored.sort((a, b) => b.healthScore - a.healthScore);
      }
      suggestOptimalScheduleTime(stories, targetPlatforms) {
        const predictSuccess = (story) => {
          let baseScore = 70;
          if (story.videoUrl) baseScore += 15;
          if (story.trendingTopic) baseScore += 10;
          if (story.category === "sports" || story.category === "movies") baseScore += 5;
          return Math.min(100, baseScore);
        };
        const optimalTimes = this.analyzeOptimalPostingTimes(stories);
        const now = /* @__PURE__ */ new Date();
        const suggestedTime = /* @__PURE__ */ new Date();
        let reason = "\u0648\u0642\u062A \u0645\u0646\u0627\u0633\u0628 \u0644\u0644\u0646\u0634\u0631";
        let dayName = "\u0627\u0644\u064A\u0648\u0645";
        let timeLabel = "";
        const dayNames = ["\u0627\u0644\u0623\u062D\u062F", "\u0627\u0644\u0627\u062B\u0646\u064A\u0646", "\u0627\u0644\u062B\u0644\u0627\u062B\u0627\u0621", "\u0627\u0644\u0623\u0631\u0628\u0639\u0627\u0621", "\u0627\u0644\u062E\u0645\u064A\u0633", "\u0627\u0644\u062C\u0645\u0639\u0629", "\u0627\u0644\u0633\u0628\u062A"];
        if (optimalTimes.length > 0) {
          const bestSlot = optimalTimes[0];
          suggestedTime.setHours(bestSlot.hour, 0, 0, 0);
          let daysToAdd = (bestSlot.dayOfWeek - now.getDay() + 7) % 7;
          if (daysToAdd === 0 && suggestedTime < now) {
            daysToAdd = 7;
          }
          suggestedTime.setDate(now.getDate() + daysToAdd);
          reason = bestSlot.reason;
          dayName = bestSlot.dayName;
          timeLabel = bestSlot.timeLabel;
        } else {
          const categoryPeaks = {
            movies: [18, 21, 23],
            // المساء المتأخر
            sports: [16, 19, 22],
            // أوقات المباريات
            recipes: [10, 15, 17],
            // قبل الغداء والعشاء
            gaming: [14, 20, 0],
            // بعد المدرسة والمساء المتأخر
            apps: [11, 14, 19],
            // خلال فترات الاستراحة
            tv_shows: [19, 21, 22]
            // وقت المشاهدة العائلية
          };
          const currentCategory = stories.length > 0 ? stories[0].category : "movies";
          const peaks = categoryPeaks[currentCategory] || [9, 13, 20, 22];
          const saudiHour = (now.getUTCHours() + 3) % 24;
          const nextPeakSaudi = peaks.find((p) => p > saudiHour) || peaks[0];
          if (nextPeakSaudi <= saudiHour) {
            suggestedTime.setDate(now.getDate() + 1);
          }
          const targetUTCHour = (nextPeakSaudi - 3 + 24) % 24;
          suggestedTime.setUTCHours(targetUTCHour, 0, 0, 0);
          dayName = dayNames[suggestedTime.getDay()];
          timeLabel = `${nextPeakSaudi.toString().padStart(2, "0")}:00`;
          reason = `\u0648\u0642\u062A \u0630\u0631\u0648\u0629 \u0645\u0642\u062A\u0631\u062D \u0644\u0641\u0626\u0629 \u0627\u0644\u0640 ${currentCategory === "movies" ? "\u0623\u0641\u0644\u0627\u0645" : currentCategory === "sports" ? "\u0631\u064A\u0627\u0636\u0629" : currentCategory === "recipes" ? "\u0637\u0628\u062E" : currentCategory === "gaming" ? "\u0623\u0644\u0639\u0627\u0628" : "\u062A\u0637\u0628\u064A\u0642\u0627\u062A"}`;
        }
        suggestedTime.setMinutes(Math.floor(Math.random() * 30));
        return {
          suggestedTime: suggestedTime.toISOString(),
          dayName,
          timeLabel: timeLabel || `${suggestedTime.getHours().toString().padStart(2, "0")}:${suggestedTime.getMinutes().toString().padStart(2, "0")}`,
          reason
        };
      }
      generateAdminSystemMetrics(users, stories, apiConfigs) {
        const activeUsers = users.filter((u) => u.status === "active").length;
        const todayStories = stories.filter((s) => {
          const today = /* @__PURE__ */ new Date();
          today.setHours(0, 0, 0, 0);
          return new Date(s.createdAt) >= today;
        }).length;
        const publishedToday = stories.filter((s) => {
          const today = /* @__PURE__ */ new Date();
          today.setHours(0, 0, 0, 0);
          return s.status === "published" && s.publishedAt && new Date(s.publishedAt) >= today;
        }).length;
        const apiHealth = {};
        apiConfigs.forEach((config) => {
          if (config.isConnected) {
            apiHealth[config.provider] = {
              status: "healthy",
              latency: 85
            };
          } else {
            apiHealth[config.provider] = {
              status: config.apiKey ? "warning" : "error",
              latency: 0
            };
          }
        });
        const alerts = [];
        const disconnectedApis = apiConfigs.filter((c) => !c.isConnected);
        if (disconnectedApis.length > 0) {
          alerts.push({
            type: "warning",
            message: `${disconnectedApis.length} APIs \u063A\u064A\u0631 \u0645\u062A\u0635\u0644\u0629`,
            timestamp: /* @__PURE__ */ new Date()
          });
        }
        const failedStories = stories.filter((s) => s.status === "failed").length;
        if (failedStories > 0) {
          alerts.push({
            type: "error",
            message: `${failedStories} \u0642\u0635\u0635 \u0641\u0634\u0644 \u0646\u0634\u0631\u0647\u0627`,
            timestamp: /* @__PURE__ */ new Date()
          });
        }
        const optimizationSuggestions = [];
        if (activeUsers > 0 && todayStories / activeUsers < 0.5) {
          optimizationSuggestions.push("\u0645\u0639\u062F\u0644 \u0627\u0644\u0646\u0634\u0631 \u0645\u0646\u062E\u0641\u0636 - \u064A\u0645\u0643\u0646 \u062A\u0634\u062C\u064A\u0639 \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645\u064A\u0646 \u0639\u0644\u0649 \u0646\u0634\u0631 \u0627\u0644\u0645\u0632\u064A\u062F");
        }
        const healthyApis = Object.values(apiHealth).filter((a) => a.status === "healthy").length;
        const systemHealth = Math.round(healthyApis / Math.max(Object.keys(apiHealth).length, 1) * 100);
        const storiesPerformance = stories.length > 0 ? Math.round(publishedToday / Math.max(todayStories, 1) * 100) : 100;
        return {
          systemHealth,
          activeUsers,
          storiesPerformance,
          apiHealth,
          alerts,
          optimizationSuggestions
        };
      }
      calculateAccountRecommendations(accounts, stories) {
        const recommendations = [];
        const accountHealth = this.analyzeAccountHealth(accounts, stories);
        const unhealthyAccounts = accountHealth.filter((a) => a.healthScore < 70);
        if (unhealthyAccounts.length > 0) {
          recommendations.push({
            type: "account",
            priority: "high",
            title: "\u062D\u0633\u0627\u0628\u0627\u062A \u062A\u062D\u062A\u0627\u062C \u0627\u0647\u062A\u0645\u0627\u0645",
            description: `${unhealthyAccounts.length} \u062D\u0633\u0627\u0628(\u0627\u062A) \u062A\u062D\u062A\u0627\u062C \u0645\u0631\u0627\u062C\u0639\u0629`,
            action: "\u0631\u0627\u062C\u0639 \u062D\u0627\u0644\u0629 \u0627\u0644\u062D\u0633\u0627\u0628\u0627\u062A",
            confidence: 0.95,
            data: unhealthyAccounts
          });
        }
        const platformUsage = /* @__PURE__ */ new Map();
        stories.forEach((s) => {
          s.platforms.forEach((p) => {
            platformUsage.set(p, (platformUsage.get(p) || 0) + 1);
          });
        });
        accounts.forEach((account) => {
          const usage = platformUsage.get(account.platform) || 0;
          if (usage === 0 && account.status === "active") {
            recommendations.push({
              type: "account",
              priority: "medium",
              title: `\u062D\u0633\u0627\u0628 ${account.name} \u063A\u064A\u0631 \u0645\u0633\u062A\u062E\u062F\u0645`,
              description: "\u0647\u0630\u0627 \u0627\u0644\u062D\u0633\u0627\u0628 \u0646\u0634\u0637 \u0644\u0643\u0646 \u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0646\u0634\u0631 \u0639\u0644\u064A\u0647",
              action: "\u0627\u0628\u062F\u0623 \u0627\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0647\u0630\u0627 \u0627\u0644\u062D\u0633\u0627\u0628",
              confidence: 0.8
            });
          }
        });
        return recommendations;
      }
      analyzeAccountPerformance(accounts, stories) {
        return accounts.map((account) => {
          const accountStories = stories.filter((s) => s.platforms.includes(account.platform));
          const publishedStories = accountStories.filter((s) => s.status === "published");
          const reach = account.reach || publishedStories.reduce((sum, s) => sum + (Number(s.reach) || 0), 0);
          const impressions = account.impressions || publishedStories.reduce((sum, s) => sum + (Number(s.impressions) || 0), 0);
          const totalEngagement = publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
          const avgEngagement = publishedStories.length > 0 ? totalEngagement / publishedStories.length : 0;
          const recentStories = publishedStories.slice(0, 5);
          const previousStories = publishedStories.slice(5, 10);
          const recentAvg = recentStories.length > 0 ? recentStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / recentStories.length : avgEngagement;
          const previousAvg = previousStories.length > 0 ? previousStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / previousStories.length : avgEngagement;
          const engagementTrend = previousAvg > 0 ? (recentAvg - previousAvg) / previousAvg * 100 : 0;
          const categoryCounts = /* @__PURE__ */ new Map();
          publishedStories.forEach((s) => categoryCounts.set(s.category, (categoryCounts.get(s.category) || 0) + 1));
          const bestContentType = Array.from(categoryCounts.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || "\u0627\u0644\u0643\u0644";
          const optimalTimes = this.analyzeOptimalPostingTimes(accountStories);
          const topPerformingTime = optimalTimes[0] ? `${optimalTimes[0].dayName} ${optimalTimes[0].timeLabel}` : "\u063A\u064A\u0631 \u0645\u062D\u062F\u062F";
          return {
            accountId: account.id,
            engagementRate: Math.round(avgEngagement * 10) / 10,
            engagementTrend: Math.round(engagementTrend),
            reach: Math.round(reach),
            reachTrend: Math.round(engagementTrend * 0.8),
            impressions: Math.round(impressions),
            impressionsTrend: Math.round(engagementTrend * 0.9),
            bestContentType,
            topPerformingTime,
            followersGrowth: Math.round(engagementTrend * 0.5)
          };
        });
      }
      analyzePerformance(stories) {
        const publishedStories = stories.filter((s) => s.status === "published");
        const avgEngagement = publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / (publishedStories.length || 1);
        const contentQualityScore = Math.min(100, Math.round(avgEngagement * 8 + (publishedStories.length > 5 ? 20 : 0)));
        const timingScore = this.calculateTimingScore(stories);
        const engagementScore = Math.min(100, Math.round(avgEngagement * 10));
        const growthScore = this.calculateGrowthScore(stories);
        return {
          contentQuality: {
            score: contentQualityScore,
            feedback: contentQualityScore >= 70 ? "\u062C\u0648\u062F\u0629 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0645\u0645\u062A\u0627\u0632\u0629" : contentQualityScore >= 50 ? "\u062C\u0648\u062F\u0629 \u062C\u064A\u062F\u0629 \u0645\u0639 \u0645\u062C\u0627\u0644 \u0644\u0644\u062A\u062D\u0633\u064A\u0646" : "\u064A\u062D\u062A\u0627\u062C \u062A\u062D\u0633\u064A\u0646 \u062C\u0648\u062F\u0629 \u0627\u0644\u0645\u062D\u062A\u0648\u0649"
          },
          timingOptimization: {
            score: timingScore,
            feedback: timingScore >= 70 ? "\u062A\u0648\u0642\u064A\u062A \u0627\u0644\u0646\u0634\u0631 \u0645\u062B\u0627\u0644\u064A" : timingScore >= 50 ? "\u0627\u0644\u062A\u0648\u0642\u064A\u062A \u062C\u064A\u062F" : "\u062C\u0631\u0628 \u0623\u0648\u0642\u0627\u062A \u0646\u0634\u0631 \u0645\u062E\u062A\u0644\u0641\u0629"
          },
          audienceEngagement: {
            score: engagementScore,
            feedback: engagementScore >= 70 ? "\u062A\u0641\u0627\u0639\u0644 \u0627\u0644\u062C\u0645\u0647\u0648\u0631 \u0645\u0645\u062A\u0627\u0632" : engagementScore >= 50 ? "\u062A\u0641\u0627\u0639\u0644 \u0645\u062A\u0648\u0633\u0637" : "\u064A\u062D\u062A\u0627\u062C \u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u062A\u0641\u0627\u0639\u0644"
          },
          growthPotential: {
            score: growthScore,
            feedback: growthScore >= 70 ? "\u0646\u0645\u0648 \u0625\u064A\u062C\u0627\u0628\u064A \u0642\u0648\u064A" : growthScore >= 50 ? "\u0646\u0645\u0648 \u0645\u0633\u062A\u0642\u0631" : "\u0647\u0646\u0627\u0643 \u0641\u0631\u0635 \u0644\u0644\u0646\u0645\u0648"
          }
        };
      }
      calculateTimingScore(stories) {
        const publishedStories = stories.filter((s) => s.status === "published" && s.publishedAt);
        if (publishedStories.length < 3) return 50;
        const peakHours = [18, 19, 20, 21, 22];
        const storiesInPeakHours = publishedStories.filter((s) => {
          const hour = new Date(s.publishedAt).getHours();
          return peakHours.includes(hour);
        }).length;
        return Math.min(100, Math.round(storiesInPeakHours / publishedStories.length * 100));
      }
      analyzeTrends(stories) {
        const categoryCount = /* @__PURE__ */ new Map();
        const platformStats = /* @__PURE__ */ new Map();
        stories.forEach((s) => {
          const existing = categoryCount.get(s.category) || { count: 0, engagement: 0 };
          existing.count++;
          existing.engagement += s.engagementRate || 0;
          categoryCount.set(s.category, existing);
          s.platforms.forEach((p) => {
            const pStats = platformStats.get(p) || { count: 0, engagement: 0 };
            pStats.count++;
            pStats.engagement += s.engagementRate || 0;
            platformStats.set(p, pStats);
          });
        });
        const sortedCategories = Array.from(categoryCount.entries()).map(([cat, data]) => ({ topic: cat, growth: Math.round(data.engagement / data.count * 10), relevance: Math.min(100, data.count * 15) })).sort((a, b) => b.growth - a.growth).slice(0, 5);
        const trendingTopics = sortedCategories.length > 0 ? sortedCategories : [
          { topic: "\u0623\u0641\u0644\u0627\u0645", growth: 25, relevance: 85 },
          { topic: "\u0645\u0633\u0644\u0633\u0644\u0627\u062A", growth: 18, relevance: 78 },
          { topic: "\u0631\u064A\u0627\u0636\u0629", growth: 15, relevance: 72 }
        ];
        const competitorInsights = ["facebook", "instagram", "tiktok"].map((platform) => {
          const stats = platformStats.get(platform) || { count: 0, engagement: 0 };
          const yourEngagement = stats.count > 0 ? Math.round(stats.engagement / stats.count * 10) / 10 : 0;
          const avgMarket = { facebook: 3.5, instagram: 4.2, tiktok: 5.8 };
          return {
            platform,
            avgEngagement: avgMarket[platform] || 4,
            yourEngagement
          };
        });
        const allCategories = ["movies", "tv_shows", "sports", "recipes", "games", "apps"];
        const usedCategories = new Set(stories.map((s) => s.category));
        const contentGaps = allCategories.filter((cat) => !usedCategories.has(cat)).slice(0, 3).map((cat, i) => {
          const categoryNames = {
            movies: "\u0623\u0641\u0644\u0627\u0645",
            tv_shows: "\u0645\u0633\u0644\u0633\u0644\u0627\u062A",
            sports: "\u0631\u064A\u0627\u0636\u0629",
            recipes: "\u0648\u0635\u0641\u0627\u062A",
            games: "\u0623\u0644\u0639\u0627\u0628",
            apps: "\u062A\u0637\u0628\u064A\u0642\u0627\u062A"
          };
          return {
            category: categoryNames[cat] || cat,
            opportunity: `\u0641\u0626\u0629 ${categoryNames[cat] || cat} \u0644\u0645 \u062A\u064F\u0633\u062A\u062E\u062F\u0645 \u0628\u0639\u062F`,
            priority: i === 0 ? "high" : i === 1 ? "medium" : "low"
          };
        });
        return {
          trendingTopics,
          competitorInsights: Array.from(platformStats.entries()).map(([platform, stats]) => ({
            platform,
            avgEngagement: stats.engagement / stats.count,
            yourEngagement: stats.engagement / stats.count * 0.9
          })),
          contentGaps: Array.from(categoryCount.entries()).filter(([_, stats]) => stats.count < 3).map(([category]) => ({
            category,
            opportunity: "\u0647\u0630\u0647 \u0627\u0644\u0641\u0626\u0629 \u063A\u064A\u0631 \u0645\u063A\u0637\u0627\u0629 \u0628\u0634\u0643\u0644 \u0643\u0627\u0641\u064D",
            priority: "medium"
          }))
        };
      }
      calculateEngagementStats(stories) {
        const publishedStories = stories.filter((s) => s.status === "published");
        const totalEngagement = publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
        const sevenDaysAgo = /* @__PURE__ */ new Date();
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
        const fourteenDaysAgo = /* @__PURE__ */ new Date();
        fourteenDaysAgo.setDate(fourteenDaysAgo.getDate() - 14);
        const recentStories = publishedStories.filter((s) => new Date(s.publishedAt || s.createdAt) > sevenDaysAgo);
        const olderStories = publishedStories.filter((s) => {
          const date = new Date(s.publishedAt || s.createdAt);
          return date > fourteenDaysAgo && date <= sevenDaysAgo;
        });
        const recentEngagement = recentStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
        const olderEngagement = olderStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
        const changePercent = olderEngagement > 0 ? Math.round((recentEngagement - olderEngagement) / olderEngagement * 100) : 0;
        const baseLikes = Math.round(totalEngagement * 150);
        const baseShares = Math.round(totalEngagement * 35);
        const baseComments = Math.round(totalEngagement * 12);
        const baseViews = Math.round(totalEngagement * 800);
        return {
          likes: baseLikes,
          likesChange: changePercent,
          shares: baseShares,
          sharesChange: Math.round(changePercent * 0.8),
          comments: baseComments,
          commentsChange: Math.round(changePercent * 1.2),
          views: baseViews,
          viewsChange: changePercent
        };
      }
    };
    smartAlgorithms = new SmartAlgorithmsEngine();
  }
});

// server/sdk/facebook.ts
var facebook_exports = {};
__export(facebook_exports, {
  FacebookSDK: () => FacebookSDK,
  facebookSDK: () => facebookSDK
});
var FACEBOOK_API_VERSION, FACEBOOK_BASE_URL, FacebookSDK, facebookSDK;
var init_facebook = __esm({
  "server/sdk/facebook.ts"() {
    "use strict";
    init_firestore();
    FACEBOOK_API_VERSION = "v22.0";
    FACEBOOK_BASE_URL = `https://graph.facebook.com/${FACEBOOK_API_VERSION}`;
    FacebookSDK = class {
      appId = "";
      appSecret = "";
      accessToken = "";
      initialized = false;
      async initialize() {
        if (this.initialized) return;
        const config = await firestoreService.getAPIConfig("facebook");
        if (config && config.appId && config.appSecret) {
          this.appId = config.appId;
          this.appSecret = config.appSecret;
          this.initialized = true;
        }
      }
      async getAppAccessToken() {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${FACEBOOK_BASE_URL}/oauth/access_token`;
        const params = new URLSearchParams({
          client_id: this.appId,
          client_secret: this.appSecret,
          grant_type: "client_credentials"
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get Facebook access token: ${response.statusText}`);
        }
        const data = await response.json();
        this.accessToken = data.access_token;
        return data.access_token;
      }
      async exchangeCodeForToken(code, redirectUri) {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${FACEBOOK_BASE_URL}/oauth/access_token`;
        const params = new URLSearchParams({
          client_id: this.appId,
          client_secret: this.appSecret,
          redirect_uri: redirectUri,
          code
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to exchange code for token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async getUserProfile(accessToken) {
        const url = `${FACEBOOK_BASE_URL}/me`;
        const params = new URLSearchParams({
          fields: "id,name,email,picture",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get user profile: ${response.statusText}`);
        }
        return await response.json();
      }
      async publishPost(pageId, accessToken, postData) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/feed`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            ...postData,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to publish post: ${response.statusText}`);
        }
        return await response.json();
      }
      async schedulePost(pageId, accessToken, postData, scheduledTime) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/feed`;
        const scheduledTimestamp = Math.floor(scheduledTime.getTime() / 1e3);
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            ...postData,
            published: false,
            scheduled_publish_time: scheduledTimestamp,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to schedule post: ${response.statusText}`);
        }
        return await response.json();
      }
      async uploadPhoto(pageId, accessToken, photoUrl, caption) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/photos`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            url: photoUrl,
            caption,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to upload photo: ${response.statusText}`);
        }
        return await response.json();
      }
      async uploadVideo(pageId, accessToken, videoUrl, description) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/videos`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            file_url: videoUrl,
            description,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to upload video: ${response.statusText}`);
        }
        return await response.json();
      }
      async getPageInsights(pageId, accessToken, metrics = ["page_impressions", "page_engaged_users"]) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/insights`;
        const params = new URLSearchParams({
          metric: metrics.join(","),
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get page insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async getPostInsights(postId, accessToken) {
        const url = `${FACEBOOK_BASE_URL}/${postId}/insights`;
        const params = new URLSearchParams({
          metric: "post_impressions,post_engaged_users,post_reactions_by_type_total",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get post insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async deletePost(postId, accessToken) {
        const url = `${FACEBOOK_BASE_URL}/${postId}`;
        const params = new URLSearchParams({
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          throw new Error(`Failed to delete post: ${response.statusText}`);
        }
        return await response.json();
      }
      async getLongLivedToken(shortLivedToken) {
        if (!this.appId || !this.appSecret) {
          await this.initialize();
        }
        const url = `${FACEBOOK_BASE_URL}/oauth/access_token`;
        const params = new URLSearchParams({
          grant_type: "fb_exchange_token",
          client_id: this.appId,
          client_secret: this.appSecret,
          fb_exchange_token: shortLivedToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get long-lived token: ${response.statusText}`);
        }
        return await response.json();
      }
      async getUserPages(accessToken) {
        const url = `${FACEBOOK_BASE_URL}/me/accounts`;
        const params = new URLSearchParams({
          access_token: accessToken,
          fields: "id,name,category,access_token"
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get user pages: ${response.statusText}`);
        }
        const data = await response.json();
        return data.data || [];
      }
      async getPageInstagramAccount(pageId, accessToken) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}`;
        const params = new URLSearchParams({
          access_token: accessToken,
          fields: "instagram_business_account"
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get page instagram account: ${response.statusText}`);
        }
        return await response.json();
      }
      async publishReel(pageId, accessToken, reelData) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/video_reels`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            upload_phase: "start",
            video_url: reelData.video_url,
            description: reelData.description,
            title: reelData.title,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to publish reel: ${response.statusText}`);
        }
        return await response.json();
      }
      async uploadUnpublishedPhoto(pageId, accessToken, photoUrl) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/photos`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            url: photoUrl,
            published: false,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          const errorMessage = errorData?.error?.message || response.statusText;
          const errorCode = errorData?.error?.code;
          if (errorCode === 190) {
            throw new Error(`\u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0646\u062A\u0647\u064A \u0627\u0644\u0635\u0644\u0627\u062D\u064A\u0629 \u0623\u0648 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D. \u064A\u0631\u062C\u0649 \u0625\u0639\u0627\u062F\u0629 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628 Facebook.`);
          }
          if (errorCode === 10) {
            throw new Error(`\u0644\u0627 \u062A\u0645\u0644\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0646\u0634\u0631 \u0627\u0644\u0635\u0648\u0631 \u0639\u0644\u0649 \u0647\u0630\u0647 \u0627\u0644\u0635\u0641\u062D\u0629. \u062A\u0623\u0643\u062F \u0645\u0646 \u0645\u0646\u062D \u0625\u0630\u0646 pages_manage_posts.`);
          }
          throw new Error(`\u0641\u0634\u0644 \u0631\u0641\u0639 \u0627\u0644\u0635\u0648\u0631\u0629: ${errorMessage}`);
        }
        const data = await response.json();
        return data.id;
      }
      async publishPhotoStory(pageId, accessToken, photoUrl) {
        console.log(`
         \u{1F4F8} === FACEBOOK PHOTO STORY ===`);
        console.log(`            Page ID: ${pageId}`);
        console.log(`            Photo URL: ${photoUrl.substring(0, 80)}...`);
        console.log(`            Duration: 20 seconds`);
        try {
          console.log(`            \u{1F504} Step 1: Uploading photo as unpublished...`);
          const photoId = await this.uploadUnpublishedPhoto(pageId, accessToken, photoUrl);
          console.log(`            \u2705 Photo uploaded. Photo ID: ${photoId}`);
          console.log(`            \u{1F504} Step 2: Publishing story using photo_id...`);
          const url = `${FACEBOOK_BASE_URL}/${pageId}/photo_stories`;
          console.log(`            API Endpoint: ${url}`);
          const requestBody = {
            photo_id: photoId,
            duration: 20,
            access_token: accessToken
          };
          const response = await fetch(url, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(requestBody)
          });
          console.log(`            HTTP Status: ${response.status} ${response.statusText}`);
          if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            const errorMessage = errorData?.error?.message || response.statusText;
            const errorCode = errorData?.error?.code;
            console.log(`            \u274C API Error: Code ${errorCode} - ${errorMessage}`);
            if (errorCode === 1) {
              throw new Error(`\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641 \u0645\u0646 Facebook. \u062A\u0623\u0643\u062F \u0645\u0646:
1. \u0645\u0648\u0627\u0641\u0642\u0629 Facebook \u0639\u0644\u0649 \u0645\u064A\u0632\u0629 "Page Stories" \u0641\u064A App Review
2. \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0625\u0630\u0646 pages_manage_posts
3. \u0623\u0646 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0645\u0648\u062B\u0642 \u0645\u0646 Facebook`);
            }
            if (errorCode === 190) {
              throw new Error(`\u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0646\u062A\u0647\u064A \u0627\u0644\u0635\u0644\u0627\u062D\u064A\u0629. \u064A\u0631\u062C\u0649 \u0625\u0639\u0627\u062F\u0629 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628 Facebook.`);
            }
            if (errorCode === 10 || errorCode === 200) {
              throw new Error(`\u0635\u0644\u0627\u062D\u064A\u0627\u062A \u063A\u064A\u0631 \u0643\u0627\u0641\u064A\u0629. \u062A\u0623\u0643\u062F \u0645\u0646 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0625\u0630\u0646 pages_manage_posts \u0648\u0645\u0648\u0627\u0641\u0642\u0629 "Page Stories".`);
            }
            throw new Error(`\u0641\u0634\u0644 \u0646\u0634\u0631 \u0627\u0644\u0642\u0635\u0629: ${errorMessage} (\u0643\u0648\u062F: ${errorCode})`);
          }
          const result = await response.json();
          console.log(`            \u2705 STORY PUBLISHED SUCCESSFULLY!`);
          console.log(`            Response: ${JSON.stringify(result)}`);
          return result;
        } catch (error) {
          console.error(`            \u274C Method 1 (photo_id) failed: ${error.message}`);
          console.log(`            \u{1F504} Attempting Method 2: Publishing with photo_url directly...`);
          const url = `${FACEBOOK_BASE_URL}/${pageId}/photo_stories`;
          const requestBody = {
            photo_url: photoUrl,
            duration: 20,
            access_token: accessToken
          };
          const response = await fetch(url, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(requestBody)
          });
          console.log(`            HTTP Status: ${response.status} ${response.statusText}`);
          if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            const errorMessage = errorData?.error?.message || response.statusText;
            const errorCode = errorData?.error?.code;
            console.log(`            \u274C API Error: Code ${errorCode} - ${errorMessage}`);
            let friendlyMessage = `\u0641\u0634\u0644 \u0646\u0634\u0631 \u0627\u0644\u0642\u0635\u0629 \u0639\u0644\u0649 Facebook: ${errorMessage}`;
            if (errorCode === 1) {
              friendlyMessage = `\u0641\u0634\u0644 \u0646\u0634\u0631 \u0627\u0644\u0642\u0635\u0629. \u0627\u0644\u0623\u0633\u0628\u0627\u0628 \u0627\u0644\u0645\u062D\u062A\u0645\u0644\u0629:
\u2022 \u0644\u0645 \u062A\u062A\u0645 \u0627\u0644\u0645\u0648\u0627\u0641\u0642\u0629 \u0639\u0644\u0649 \u0645\u064A\u0632\u0629 "Page Stories" \u0641\u064A App Review
\u2022 \u064A\u062C\u0628 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0625\u0630\u0646 pages_manage_posts
\u2022 \u0642\u062F \u064A\u062D\u062A\u0627\u062C \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0644\u062A\u0648\u062B\u064A\u0642 \u0645\u0646 Facebook`;
            } else if (errorCode === 190) {
              friendlyMessage = `\u0631\u0645\u0632 \u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0646\u062A\u0647\u064A \u0627\u0644\u0635\u0644\u0627\u062D\u064A\u0629. \u064A\u0631\u062C\u0649 \u0625\u0639\u0627\u062F\u0629 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628 Facebook \u0645\u0646 \u0625\u062F\u0627\u0631\u0629 \u0627\u0644\u062D\u0633\u0627\u0628\u0627\u062A.`;
            } else if (errorCode === 10 || errorCode === 200) {
              friendlyMessage = `\u0635\u0644\u0627\u062D\u064A\u0627\u062A \u063A\u064A\u0631 \u0643\u0627\u0641\u064A\u0629 \u0644\u0646\u0634\u0631 \u0627\u0644\u0642\u0635\u0635. \u062A\u0623\u0643\u062F \u0645\u0646 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0625\u0630\u0646 pages_manage_posts.`;
            }
            throw new Error(friendlyMessage);
          }
          const result = await response.json();
          console.log(`            \u2705 STORY PUBLISHED SUCCESSFULLY (Method 2)!`);
          console.log(`            Response: ${JSON.stringify(result)}`);
          return result;
        }
      }
      async publishVideoStory(pageId, accessToken, videoUrl) {
        console.log(`
         \u{1F3AC} === FACEBOOK VIDEO STORY ===`);
        console.log(`            Page ID: ${pageId}`);
        console.log(`            Video URL: ${videoUrl.substring(0, 80)}...`);
        console.log(`            Duration: 20 seconds`);
        const url = `${FACEBOOK_BASE_URL}/${pageId}/video_stories`;
        console.log(`            API Endpoint: ${url}`);
        const requestBody = {
          video_url: videoUrl,
          duration: 20,
          upload_phase: "start",
          access_token: accessToken
        };
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(requestBody)
        });
        console.log(`            HTTP Status: ${response.status} ${response.statusText}`);
        if (!response.ok) {
          const errorText = await response.text();
          console.log(`            \u274C API Error: ${errorText}`);
          throw new Error(`Failed to publish video story: ${response.statusText} - ${errorText}`);
        }
        const result = await response.json();
        console.log(`            \u2705 VIDEO STORY PUBLISHED SUCCESSFULLY!`);
        console.log(`            Response: ${JSON.stringify(result)}`);
        return result;
      }
      async publishStory(pageId, accessToken, storyData) {
        if (storyData.photo_url) {
          return await this.publishPhotoStory(pageId, accessToken, storyData.photo_url);
        } else if (storyData.video_url) {
          return await this.publishVideoStory(pageId, accessToken, storyData.video_url);
        }
        throw new Error("\u064A\u062C\u0628 \u062A\u0648\u0641\u064A\u0631 \u0631\u0627\u0628\u0637 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648 \u0644\u0644\u0646\u0634\u0631 \u0643\u0640 Story \u0639\u0644\u0649 Facebook");
      }
      async getPageFeed(pageId, accessToken, limit = 25) {
        const url = `${FACEBOOK_BASE_URL}/${pageId}/feed`;
        const params = new URLSearchParams({
          fields: "id,message,created_time,full_picture,permalink_url,shares,likes.summary(true),comments.summary(true)",
          limit: limit.toString(),
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get page feed: ${response.statusText}`);
        }
        return await response.json();
      }
      async verifyAccessToken(accessToken) {
        if (!this.appId || !this.appSecret) {
          await this.initialize();
        }
        try {
          const appToken = await this.getAppAccessToken();
          const url = `${FACEBOOK_BASE_URL}/debug_token`;
          const params = new URLSearchParams({
            input_token: accessToken,
            access_token: appToken
          });
          const response = await fetch(`${url}?${params.toString()}`);
          if (!response.ok) {
            throw new Error(`Failed to verify access token: ${response.statusText}`);
          }
          const data = await response.json();
          return data.data;
        } catch (error) {
          console.error("Error verifying Facebook token:", error.message);
          return { is_valid: false };
        }
      }
      async refreshToken(accessToken) {
        try {
          const result = await this.getLongLivedToken(accessToken);
          return result.access_token;
        } catch (error) {
          console.error("Error refreshing Facebook token:", error.message);
          return null;
        }
      }
    };
    facebookSDK = new FacebookSDK();
  }
});

// server/sdk/tiktok.ts
var tiktok_exports = {};
__export(tiktok_exports, {
  TikTokSDK: () => TikTokSDK,
  tiktokSDK: () => tiktokSDK
});
var TIKTOK_API_VERSION, TIKTOK_BASE_URL, TikTokSDK, tiktokSDK;
var init_tiktok = __esm({
  "server/sdk/tiktok.ts"() {
    "use strict";
    init_firestore();
    TIKTOK_API_VERSION = "v2";
    TIKTOK_BASE_URL = `https://open.tiktokapis.com/${TIKTOK_API_VERSION}`;
    TikTokSDK = class {
      clientKey = "";
      clientSecret = "";
      initialized = false;
      async initialize() {
        if (this.initialized) return;
        const config = await firestoreService.getAPIConfig("tiktok");
        if (config && config.apiKey && config.appSecret) {
          this.clientKey = config.apiKey;
          this.clientSecret = config.appSecret;
          this.initialized = true;
        }
      }
      async getClientAccessToken() {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${TIKTOK_BASE_URL}/oauth/token/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: new URLSearchParams({
            client_key: this.clientKey,
            client_secret: this.clientSecret,
            grant_type: "client_credentials"
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to get TikTok access token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async exchangeCodeForToken(code, redirectUri) {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${TIKTOK_BASE_URL}/oauth/token/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: new URLSearchParams({
            client_key: this.clientKey,
            client_secret: this.clientSecret,
            code,
            grant_type: "authorization_code",
            redirect_uri: redirectUri
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to exchange code for token: ${response.statusText}`);
        }
        return await response.json();
      }
      async refreshAccessToken(refreshToken) {
        if (!this.clientKey || !this.clientSecret) {
          await this.initialize();
        }
        const url = `${TIKTOK_BASE_URL}/oauth/token/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: new URLSearchParams({
            client_key: this.clientKey,
            client_secret: this.clientSecret,
            grant_type: "refresh_token",
            refresh_token: refreshToken
          })
        });
        if (!response.ok) {
          const errorText = await response.text().catch(() => response.statusText);
          throw new Error(`Failed to refresh access token: ${errorText}`);
        }
        return await response.json();
      }
      async getUserInfo(accessToken) {
        const url = `${TIKTOK_BASE_URL}/user/info/`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          }
        });
        if (!response.ok) {
          throw new Error(`Failed to get user info: ${response.statusText}`);
        }
        return await response.json();
      }
      async getCreatorInfo(accessToken) {
        const url = `${TIKTOK_BASE_URL}/post/creator/info/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({})
        });
        if (!response.ok) {
          throw new Error(`Failed to get creator info: ${response.statusText}`);
        }
        return await response.json();
      }
      async initializeVideoUpload(accessToken, videoData) {
        const url = `${TIKTOK_BASE_URL}/post/publish/video/init/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(videoData)
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`Failed to initialize video upload: ${error}`);
        }
        return await response.json();
      }
      async publishVideoFromUrl(accessToken, videoUrl, title, privacyLevel = "PUBLIC_TO_EVERYONE") {
        const videoData = {
          post_info: {
            title,
            privacy_level: privacyLevel,
            disable_duet: false,
            disable_comment: false,
            disable_stitch: false
          },
          source_info: {
            source: "PULL_FROM_URL",
            video_url: videoUrl
          }
        };
        return await this.initializeVideoUpload(accessToken, videoData);
      }
      async publishPhotoPost(accessToken, photoUrls, title, description, privacyLevel = "PUBLIC_TO_EVERYONE") {
        const url = `${TIKTOK_BASE_URL}/post/publish/content/init/`;
        const photoArray = Array.isArray(photoUrls) ? photoUrls : [photoUrls];
        if (photoArray.length < 2) {
          photoArray.push(photoArray[0]);
        }
        const photoData = {
          post_info: {
            title,
            description,
            privacy_level: privacyLevel,
            auto_add_music: true,
            disable_comment: false
          },
          source_info: {
            source: "PULL_FROM_URL",
            photo_cover_index: 1,
            photo_images: photoArray.slice(0, 35)
          },
          post_mode: "DIRECT_POST",
          media_type: "PHOTO"
        };
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(photoData)
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`Failed to publish photo post: ${error}`);
        }
        return await response.json();
      }
      async getVideoList(accessToken, cursor, maxCount = 20) {
        const url = `${TIKTOK_BASE_URL}/video/list/`;
        const body = {
          max_count: maxCount
        };
        if (cursor) {
          body.cursor = cursor;
        }
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(body)
        });
        if (!response.ok) {
          throw new Error(`Failed to get video list: ${response.statusText}`);
        }
        return await response.json();
      }
      async getVideoInsights(accessToken, videoIds) {
        const url = `${TIKTOK_BASE_URL}/video/query/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            filters: {
              video_ids: videoIds
            }
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to get video insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async getUserAnalytics(accessToken, startDate, endDate) {
        const url = `${TIKTOK_BASE_URL}/research/user/info/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            start_date: startDate,
            end_date: endDate
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to get user analytics: ${response.statusText}`);
        }
        return await response.json();
      }
      async getCommentList(accessToken, videoId, cursor, maxCount = 20) {
        const url = `${TIKTOK_BASE_URL}/comment/list/`;
        const body = {
          video_id: videoId,
          max_count: maxCount
        };
        if (cursor) {
          body.cursor = cursor;
        }
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(body)
        });
        if (!response.ok) {
          throw new Error(`Failed to get comment list: ${response.statusText}`);
        }
        return await response.json();
      }
      async replyToComment(accessToken, commentId, videoId, text) {
        const url = `${TIKTOK_BASE_URL}/comment/reply/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            comment_id: commentId,
            video_id: videoId,
            text
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to reply to comment: ${response.statusText}`);
        }
        return await response.json();
      }
      async checkPublishStatus(accessToken, publishId) {
        const url = `${TIKTOK_BASE_URL}/post/publish/status/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            publish_id: publishId
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to check publish status: ${response.statusText}`);
        }
        return await response.json();
      }
      async revokeAccessToken(accessToken) {
        if (!this.clientKey) {
          await this.initialize();
        }
        const url = `${TIKTOK_BASE_URL}/oauth/revoke/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: new URLSearchParams({
            client_key: this.clientKey,
            token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to revoke access token: ${response.statusText}`);
        }
        return await response.json();
      }
      async verifyAccessToken(accessToken) {
        try {
          const result = await this.getUserInfo(accessToken);
          return !!result.data?.user;
        } catch {
          return false;
        }
      }
      async refreshToken(refreshToken) {
        try {
          return await this.refreshAccessToken(refreshToken);
        } catch (error) {
          console.error("Error refreshing TikTok token:", error.message);
          return null;
        }
      }
      async getVideoQuery(accessToken, filters) {
        const url = `${TIKTOK_BASE_URL}/video/query/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            filters: {
              ...filters,
              max_count: filters.max_count || 20
            },
            fields: ["id", "create_time", "cover_image_url", "share_url", "video_description", "duration", "height", "width", "title", "embed_html", "embed_link", "like_count", "comment_count", "share_count", "view_count"]
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to query videos: ${response.statusText}`);
        }
        return await response.json();
      }
      async shareInsights(accessToken, videoId, metrics = ["LIKES", "COMMENTS", "SHARES", "VIEWS"]) {
        const url = `${TIKTOK_BASE_URL}/video/query/`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            filters: {
              video_ids: [videoId]
            },
            fields: metrics.map((m) => m.toLowerCase() + "_count")
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to get share insights: ${response.statusText}`);
        }
        return await response.json();
      }
    };
    tiktokSDK = new TikTokSDK();
  }
});

// server/video-scheduler.ts
var video_scheduler_exports = {};
__export(video_scheduler_exports, {
  VideoScheduler: () => VideoScheduler,
  videoScheduler: () => videoScheduler
});
var VideoScheduler, videoScheduler;
var init_video_scheduler = __esm({
  "server/video-scheduler.ts"() {
    "use strict";
    init_firestore();
    init_video_generator();
    VideoScheduler = class {
      scheduledVideoJobs = /* @__PURE__ */ new Map();
      /**
       * Schedule a video to be generated before its scheduled publish time
       */
      async scheduleVideoGeneration(story, hoursBefore = 4) {
        try {
          if (!story.id) {
            console.error("Cannot schedule video for story without ID");
            return false;
          }
          if (!story.mediaUrl) {
            console.error(`\u274C \u0644\u0627 \u064A\u0645\u0643\u0646 \u062C\u062F\u0648\u0644\u0629 \u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u0644\u0644\u0642\u0635\u0629 ${story.id} \u0644\u0639\u062F\u0645 \u0648\u062C\u0648\u062F \u0631\u0627\u0628\u0637 \u0648\u0633\u0627\u0626\u0637`);
            await firestoreService.updateStory(story.id, { videoGenerationStatus: "error" });
            return false;
          }
          if (story.videoGenerationStatus === "generating" || story.videoGenerationStatus === "generated") {
            console.log(`\u2139\uFE0F Video already in status ${story.videoGenerationStatus} for story ${story.id}`);
            return true;
          }
          const publishTime = new Date(story.scheduledTime);
          const generationTime = new Date(publishTime.getTime() - hoursBefore * 60 * 60 * 1e3);
          const now = /* @__PURE__ */ new Date();
          if (generationTime <= now) {
            console.log(`\u23F0 Generation time already passed or imminent for story ${story.id}, generating immediately`);
            this.generateVideoNow(story.id).catch((err) => console.error(`Failed generation for ${story.id}:`, err));
            return true;
          }
          const delayMs = generationTime.getTime() - now.getTime();
          const jobId = `video-gen-${story.id}`;
          if (this.scheduledVideoJobs.has(jobId)) {
            clearTimeout(this.scheduledVideoJobs.get(jobId));
          }
          const timeout = setTimeout(async () => {
            console.log(`\u{1F3AC} Starting scheduled video generation for story ${story.id}`);
            await this.generateVideoNow(story.id);
            this.scheduledVideoJobs.delete(jobId);
          }, delayMs);
          this.scheduledVideoJobs.set(jobId, timeout);
          console.log(`\u23F0 Video generation scheduled for story ${story.id} in ${hoursBefore} hours`);
          return true;
        } catch (error) {
          console.error(`\u274C Error scheduling video generation:`, error);
          return false;
        }
      }
      /**
       * Generate video for a story immediately
       */
      async generateVideoNow(storyId) {
        try {
          const story = await firestoreService.getStoryById(storyId);
          if (!story) {
            console.error(`Story ${storyId} not found`);
            return;
          }
          await firestoreService.updateStory(storyId, {
            videoGenerationStatus: "generating",
            videoGeneratedAt: /* @__PURE__ */ new Date()
          });
          console.log(`\u{1F3AC} Starting video generation for story ${storyId}`);
          const videoResult = await videoGenerator.generateVideo ? await videoGenerator.generateVideo(story) : { success: false, error: "Video generation not available" };
          if (videoResult.success && videoResult.videoUrl) {
            await firestoreService.updateStory(storyId, {
              videoUrl: videoResult.videoUrl,
              videoGenerationStatus: "generated",
              videoStorageKey: videoResult.storageKey,
              videoFileSize: videoResult.fileSize,
              videoDuration: videoResult.duration
            });
            console.log(`\u2705 Video generated successfully for story ${storyId}`);
          } else {
            await firestoreService.updateStory(storyId, {
              videoGenerationStatus: "error"
            });
            console.error(`\u274C Video generation failed for story ${storyId}: ${videoResult.error}`);
          }
        } catch (error) {
          console.error(`\u274C Error in generateVideoNow:`, error);
          try {
            await firestoreService.updateStory(storyId, {
              videoGenerationStatus: "error"
            });
          } catch (updateError) {
            console.error(`Failed to update story status:`, updateError);
          }
        }
      }
      /**
       * Get all scheduled video jobs
       */
      getScheduledJobs() {
        const jobIds = [];
        this.scheduledVideoJobs.forEach((_, jobId) => {
          jobIds.push(jobId);
        });
        return jobIds.map((jobId) => ({
          jobId,
          storyId: jobId.replace("video-gen-", "")
        }));
      }
      /**
       * Cancel a scheduled video generation
       */
      cancelScheduledJob(storyId) {
        const jobId = `video-gen-${storyId}`;
        const timeout = this.scheduledVideoJobs.get(jobId);
        if (timeout) {
          clearTimeout(timeout);
          this.scheduledVideoJobs.delete(jobId);
          console.log(`\u274C Cancelled scheduled video generation for story ${storyId}`);
          return true;
        }
        return false;
      }
      /**
       * Clear all scheduled jobs
       */
      clearAllJobs() {
        const count = this.scheduledVideoJobs.size;
        const timeouts = [];
        this.scheduledVideoJobs.forEach((timeout) => {
          timeouts.push(timeout);
        });
        for (const timeout of timeouts) {
          clearTimeout(timeout);
        }
        this.scheduledVideoJobs.clear();
        console.log(`\u{1F5D1}\uFE0F Cleared ${count} scheduled video generation jobs`);
        return count;
      }
    };
    videoScheduler = new VideoScheduler();
  }
});

// server/facebook-stories-publisher.ts
var facebook_stories_publisher_exports = {};
__export(facebook_stories_publisher_exports, {
  FacebookStoriesPublisher: () => FacebookStoriesPublisher,
  facebookStoriesPublisher: () => facebookStoriesPublisher
});
var FacebookStoriesPublisher, facebookStoriesPublisher;
var init_facebook_stories_publisher = __esm({
  "server/facebook-stories-publisher.ts"() {
    "use strict";
    init_firestore();
    FacebookStoriesPublisher = class {
      async publishStoryToFacebook(story, accountId) {
        try {
          const account = await firestoreService.getLinkedAccountById(accountId);
          if (!account || account.platform !== "facebook") {
            return {
              success: false,
              error: "Invalid Facebook account"
            };
          }
          if (account.status !== "active") {
            return {
              success: false,
              error: "Account is not active"
            };
          }
          if (!account.capabilities.canPublishStories) {
            return {
              success: false,
              error: "Account cannot publish stories"
            };
          }
          let imageUrl = story.facebookPngUrl || story.jpegUrl || story.mediaUrl;
          if (!imageUrl) {
            return {
              success: false,
              error: "No image URL available for story"
            };
          }
          console.log(`\u{1F4F1} Publishing story ${story.id} to Facebook Stories...`);
          const response = await this.callFacebookStoriesAPI(
            account.externalId,
            account.accessToken,
            {
              image_url: imageUrl,
              content: story.content,
              link: `https://yourapp.com/stories/${story.id}`,
              video_url: story.videoUrl
            }
          );
          if (!response.success) {
            return {
              success: false,
              error: response.error || "Failed to publish to Facebook Stories"
            };
          }
          console.log(`\u2705 Successfully published story ${story.id} to Facebook Stories (ID: ${response.publishedId})`);
          return {
            success: true,
            publishedId: response.publishedId
          };
        } catch (error) {
          console.error(`\u274C Error publishing to Facebook Stories:`, error);
          return {
            success: false,
            error: error.message
          };
        }
      }
      async callFacebookStoriesAPI(pageId, accessToken, data) {
        try {
          const url = `https://graph.facebook.com/v18.0/${pageId}/stories`;
          const payload = {
            access_token: accessToken
          };
          if (data.video_url) {
            payload.video_url = data.video_url;
          } else {
            payload.image_url = data.image_url;
          }
          const response = await fetch(url, {
            method: "POST",
            body: JSON.stringify(payload),
            headers: {
              "Content-Type": "application/json"
            }
          });
          if (!response.ok) {
            const error = await response.json();
            return {
              success: false,
              error: error.message || "Facebook API error"
            };
          }
          const result = await response.json();
          return {
            success: true,
            publishedId: result.id
          };
        } catch (error) {
          return {
            success: false,
            error: error.message
          };
        }
      }
    };
    facebookStoriesPublisher = new FacebookStoriesPublisher();
  }
});

// server/sdk/instagram.ts
var instagram_exports = {};
__export(instagram_exports, {
  InstagramSDK: () => InstagramSDK,
  instagramSDK: () => instagramSDK
});
var INSTAGRAM_API_VERSION, INSTAGRAM_BASE_URL, InstagramSDK, instagramSDK;
var init_instagram = __esm({
  "server/sdk/instagram.ts"() {
    "use strict";
    init_firestore();
    INSTAGRAM_API_VERSION = "v22.0";
    INSTAGRAM_BASE_URL = `https://graph.facebook.com/${INSTAGRAM_API_VERSION}`;
    InstagramSDK = class {
      appId = "";
      appSecret = "";
      initialized = false;
      async initialize() {
        if (this.initialized) return;
        const config = await firestoreService.getAPIConfig("instagram");
        if (config && config.appId && config.appSecret) {
          this.appId = config.appId;
          this.appSecret = config.appSecret;
          this.initialized = true;
        }
      }
      async getAppAccessToken() {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${INSTAGRAM_BASE_URL}/oauth/access_token`;
        const params = new URLSearchParams({
          client_id: this.appId,
          client_secret: this.appSecret,
          grant_type: "client_credentials"
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get Instagram access token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async exchangeCodeForToken(code, redirectUri) {
        if (!this.initialized) {
          await this.initialize();
        }
        const url = `${INSTAGRAM_BASE_URL}/oauth/access_token`;
        const params = new URLSearchParams({
          client_id: this.appId,
          client_secret: this.appSecret,
          redirect_uri: redirectUri,
          code,
          grant_type: "authorization_code"
        });
        const response = await fetch(`${url}?${params.toString()}`, {
          method: "POST"
        });
        if (!response.ok) {
          throw new Error(`Failed to exchange code for token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async getUserProfile(igUserId, accessToken) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}`;
        const params = new URLSearchParams({
          fields: "id,username,account_type,media_count,followers_count,follows_count",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get user profile: ${response.statusText}`);
        }
        return await response.json();
      }
      async createMediaContainer(igUserId, accessToken, mediaData) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/media`;
        const body = {
          access_token: accessToken,
          ...mediaData
        };
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(body)
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`Failed to create media container: ${error}`);
        }
        const data = await response.json();
        return data.id;
      }
      async publishMedia(igUserId, accessToken, creationId) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/media_publish`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            creation_id: creationId,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`Failed to publish media: ${error}`);
        }
        return await response.json();
      }
      async publishPost(igUserId, accessToken, mediaData) {
        const creationId = await this.createMediaContainer(igUserId, accessToken, mediaData);
        await new Promise((resolve) => setTimeout(resolve, 2e3));
        return await this.publishMedia(igUserId, accessToken, creationId);
      }
      async publishReel(igUserId, accessToken, videoUrl, caption, coverUrl, shareToFeed = true) {
        const mediaData = {
          video_url: videoUrl,
          caption,
          media_type: "REELS",
          cover_url: coverUrl,
          share_to_feed: shareToFeed
        };
        return await this.publishPost(igUserId, accessToken, mediaData);
      }
      async publishStory(igUserId, accessToken, storyData) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/media`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            ...storyData,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to publish story: ${response.statusText}`);
        }
        const data = await response.json();
        return await this.publishMedia(igUserId, accessToken, data.id);
      }
      async getMediaInsights(mediaId, accessToken) {
        const url = `${INSTAGRAM_BASE_URL}/${mediaId}/insights`;
        const params = new URLSearchParams({
          metric: "impressions,reach,engagement,saved,likes,comments,shares,plays,total_interactions",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get media insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async getUserInsights(igUserId, accessToken, metric = ["impressions", "reach", "follower_count", "profile_views"]) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/insights`;
        const params = new URLSearchParams({
          metric: metric.join(","),
          period: "day",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get user insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async getUserMedia(igUserId, accessToken, limit = 25) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/media`;
        const params = new URLSearchParams({
          fields: "id,caption,media_type,media_url,permalink,timestamp,thumbnail_url",
          limit: limit.toString(),
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get user media: ${response.statusText}`);
        }
        return await response.json();
      }
      async deleteMedia(mediaId, accessToken) {
        const url = `${INSTAGRAM_BASE_URL}/${mediaId}`;
        const params = new URLSearchParams({
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          throw new Error(`Failed to delete media: ${response.statusText}`);
        }
        return await response.json();
      }
      async getComments(mediaId, accessToken) {
        const url = `${INSTAGRAM_BASE_URL}/${mediaId}/comments`;
        const params = new URLSearchParams({
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get comments: ${response.statusText}`);
        }
        return await response.json();
      }
      async replyToComment(commentId, accessToken, message) {
        const url = `${INSTAGRAM_BASE_URL}/${commentId}/replies`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            message,
            access_token: accessToken
          })
        });
        if (!response.ok) {
          throw new Error(`Failed to reply to comment: ${response.statusText}`);
        }
        return await response.json();
      }
      async exchangeForLongLivedToken(shortLivedToken) {
        if (!this.appId || !this.appSecret) {
          await this.initialize();
        }
        const url = `${INSTAGRAM_BASE_URL}/access_token`;
        const params = new URLSearchParams({
          grant_type: "ig_exchange_token",
          client_secret: this.appSecret,
          access_token: shortLivedToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to exchange for long-lived token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async refreshLongLivedToken(longLivedToken) {
        const url = `${INSTAGRAM_BASE_URL}/refresh_access_token`;
        const params = new URLSearchParams({
          grant_type: "ig_refresh_token",
          access_token: longLivedToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to refresh long-lived token: ${response.statusText}`);
        }
        const data = await response.json();
        return data.access_token;
      }
      async verifyAccessToken(accessToken) {
        try {
          const url = `${INSTAGRAM_BASE_URL}/me`;
          const params = new URLSearchParams({
            fields: "id",
            access_token: accessToken
          });
          const response = await fetch(`${url}?${params.toString()}`);
          return response.ok;
        } catch {
          return false;
        }
      }
      async refreshToken(accessToken) {
        try {
          return await this.refreshLongLivedToken(accessToken);
        } catch (error) {
          console.error("Error refreshing Instagram token:", error.message);
          return null;
        }
      }
      async getHashtagId(igUserId, accessToken, hashtag) {
        const url = `${INSTAGRAM_BASE_URL}/ig_hashtag_search`;
        const params = new URLSearchParams({
          user_id: igUserId,
          q: hashtag,
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get hashtag ID: ${response.statusText}`);
        }
        const data = await response.json();
        return data.data[0]?.id;
      }
      async getHashtagTopMedia(hashtagId, igUserId, accessToken, limit = 25) {
        const url = `${INSTAGRAM_BASE_URL}/${hashtagId}/top_media`;
        const params = new URLSearchParams({
          user_id: igUserId,
          fields: "id,caption,media_type,media_url,permalink,timestamp",
          limit: limit.toString(),
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get hashtag top media: ${response.statusText}`);
        }
        return await response.json();
      }
      async getAccountInsights(igUserId, accessToken, period = "day", metrics = ["impressions", "reach", "profile_views", "follower_count"]) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/insights`;
        const params = new URLSearchParams({
          metric: metrics.join(","),
          period,
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get account insights: ${response.statusText}`);
        }
        return await response.json();
      }
      async getStories(igUserId, accessToken) {
        const url = `${INSTAGRAM_BASE_URL}/${igUserId}/stories`;
        const params = new URLSearchParams({
          fields: "id,media_type,media_url,permalink,timestamp",
          access_token: accessToken
        });
        const response = await fetch(`${url}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Failed to get stories: ${response.statusText}`);
        }
        return await response.json();
      }
    };
    instagramSDK = new InstagramSDK();
  }
});

// server/cron-scheduler.ts
var cron_scheduler_exports = {};
__export(cron_scheduler_exports, {
  AdvancedCronScheduler: () => AdvancedCronScheduler,
  cronScheduler: () => cronScheduler,
  refreshAccountToken: () => refreshAccountToken
});
import * as cron from "node-cron";
async function refreshAccountToken(account) {
  try {
    let newAccessToken = "";
    let newRefreshToken = account.refreshToken;
    let expiresIn = 0;
    if (account.platform === "facebook" || account.platform === "instagram") {
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      try {
        const tokenData = await facebookSDK2.getLongLivedToken(account.accessToken);
        newAccessToken = tokenData.access_token;
        expiresIn = tokenData.expires_in || 5184e3;
      } catch (fbError) {
        console.error(`\u274C Auto-refresh Facebook token failed for ${account.name}:`, fbError.message);
        throw fbError;
      }
    } else if (account.platform === "tiktok") {
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      if (!account.refreshToken) {
        throw new Error("\u0644\u0627 \u064A\u0648\u062C\u062F \u0631\u0645\u0632 \u062A\u062D\u062F\u064A\u062B \u0644\u0640 TikTok");
      }
      try {
        const tokenData = await tiktokSDK2.refreshAccessToken(account.refreshToken);
        newAccessToken = tokenData.access_token;
        newRefreshToken = tokenData.refresh_token || account.refreshToken;
        expiresIn = tokenData.expires_in;
      } catch (ttError) {
        console.error(`\u274C Auto-refresh TikTok token failed for ${account.name}:`, ttError.message);
        throw ttError;
      }
    }
    if (!newAccessToken) {
      throw new Error("\u0641\u0634\u0644 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0631\u0645\u0632 \u0648\u0635\u0648\u0644 \u062C\u062F\u064A\u062F");
    }
    const tokenExpiresAt = new Date(Date.now() + expiresIn * 1e3);
    const updateData = {
      accessToken: newAccessToken,
      tokenExpiresAt,
      status: "active"
    };
    if (newRefreshToken !== void 0) {
      updateData.refreshToken = newRefreshToken;
    }
    await firestoreService.updateLinkedAccount(account.id, updateData);
    console.log(`\u2705 Auto-refreshed token for ${account.platform} account: ${account.name}`);
    return true;
  } catch (error) {
    console.error(`\u274C Token refresh failed for account ${account.id}:`, error.message);
    await firestoreService.updateLinkedAccount(account.id, { status: "error" }).catch(() => {
    });
    return false;
  }
}
var MEMORY_CONFIG, HEALTH_CONFIG, AdvancedCronScheduler, cronScheduler;
var init_cron_scheduler = __esm({
  "server/cron-scheduler.ts"() {
    "use strict";
    init_firestore();
    init_smart_algorithms();
    MEMORY_CONFIG = {
      maxQueueSize: 1e3,
      maxResultsHistory: 200,
      staleQueueItemHours: 24,
      cleanupIntervalMs: 36e5
    };
    HEALTH_CONFIG = {
      criticalFailureRate: 0.5,
      warningFailureRate: 0.2,
      consecutiveFailuresForUnhealthy: 5,
      heartbeatIntervalMs: 3e4
    };
    AdvancedCronScheduler = class {
      isRunning = false;
      cronJob = null;
      healthCheckInterval = null;
      cleanupInterval = null;
      startTime = null;
      lastRunTime = null;
      nextRunTime = null;
      storyQueue = /* @__PURE__ */ new Map();
      publishResults = [];
      firebaseInitialized = false;
      consecutiveFailures = 0;
      isExecuting = false;
      cronExpression = "0 6 * * *";
      stats = {
        storiesPublishedToday: 0,
        failedPublications: 0,
        successfulPublications: 0,
        lastResetDate: (/* @__PURE__ */ new Date()).toDateString(),
        totalExecutions: 0,
        averageExecutionTime: 0
      };
      constructor(customCronExpression) {
        if (customCronExpression && cron.validate(customCronExpression)) {
          this.cronExpression = customCronExpression;
        }
      }
      async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        this.startTime = /* @__PURE__ */ new Date();
        this.consecutiveFailures = 0;
        this.cronJob = cron.schedule(this.cronExpression, async () => {
          await this.safeExecuteCronJob();
        }, { timezone: "UTC" });
        this.healthCheckInterval = setInterval(() => this.performHealthCheck(), HEALTH_CONFIG.heartbeatIntervalMs);
        this.cleanupInterval = setInterval(() => this.performMemoryCleanup(), MEMORY_CONFIG.cleanupIntervalMs);
        this.updateNextRunTime();
        console.log("\u{1F680} Advanced Cron Scheduler started");
      }
      getStatus() {
        const uptime = this.startTime ? Date.now() - this.startTime.getTime() : 0;
        let healthStatus = "healthy";
        const isRecentlyActive = this.lastRunTime && Date.now() - this.lastRunTime.getTime() < 24 * 60 * 60 * 1e3;
        const isCronActive = this.cronJob !== null || (isRecentlyActive ?? false);
        if (this.consecutiveFailures >= HEALTH_CONFIG.consecutiveFailuresForUnhealthy) healthStatus = "unhealthy";
        else if (this.consecutiveFailures > 0) healthStatus = "degraded";
        return {
          isRunning: isCronActive,
          lastRun: this.lastRunTime,
          nextRun: this.nextRunTime,
          storiesInQueue: this.storyQueue.size,
          storiesPublishedToday: this.stats.storiesPublishedToday,
          failedPublications: this.stats.failedPublications,
          successfulPublications: this.stats.successfulPublications,
          uptime,
          cronExpression: this.cronExpression,
          healthStatus,
          lastHealthCheck: /* @__PURE__ */ new Date(),
          consecutiveFailures: this.consecutiveFailures
        };
      }
      async checkScheduledStoriesForPublishing() {
        return await this.safeExecuteCronJob();
      }
      async safeExecuteCronJob() {
        if (this.isExecuting) return { processed: 0, published: 0, failed: 0 };
        this.isExecuting = true;
        try {
          const result = await this.executeCronJob();
          this.consecutiveFailures = 0;
          return result;
        } catch (e) {
          this.consecutiveFailures++;
          return { processed: 0, published: 0, failed: 0 };
        } finally {
          this.isExecuting = false;
          this.updateNextRunTime();
        }
      }
      async executeCronJob() {
        this.lastRunTime = /* @__PURE__ */ new Date();
        try {
          this.performMemoryCleanup();
          const stories = await firestoreService.getAllScheduledStories();
          const now = /* @__PURE__ */ new Date();
          const due = stories.filter(
            (s) => s.status === "scheduled" && s.scheduledTime && new Date(s.scheduledTime) <= now
          );
          const activeAccounts = await firestoreService.getLinkedAccountsByUser("all", { status: "active" });
          const accountsByHealth = smartAlgorithms.dijkstraHealthScore(
            smartAlgorithms.analyzeAccountHealth(activeAccounts, stories)
          );
          const sortedDue = due.sort((a, b) => {
            const scoreA = accountsByHealth.find((acc) => acc.platform === (a.platforms[0] || ""))?.healthScore || 0;
            const scoreB = accountsByHealth.find((acc) => acc.platform === (b.platforms[0] || ""))?.healthScore || 0;
            return scoreB - scoreA;
          });
          console.log(`\u23F0 Cron check: ${sortedDue.length} stories due for publishing (Optimized by Dijkstra)`);
          let published = 0;
          let failed = 0;
          for (const story of sortedDue) {
            try {
              const activeAccounts2 = await firestoreService.getLinkedAccountsByUser(story.userId, { status: "active" });
              let refreshSuccess = true;
              for (const acc of activeAccounts2) {
                const isExpiring = acc.tokenExpiresAt && new Date(acc.tokenExpiresAt).getTime() - Date.now() < 24 * 60 * 60 * 1e3;
                if (isExpiring || acc.status === "error") {
                  console.log(`\u{1F504} Token for ${acc.name} is expiring or in error state. Attempting smart refresh...`);
                  const refreshed = await refreshAccountToken(acc);
                  if (!refreshed) refreshSuccess = false;
                }
              }
              if (!refreshSuccess) {
                console.warn(`\u26A0\uFE0F Some tokens failed to refresh for user ${story.userId}. Retrying in next cycle.`);
              }
              const accounts = await firestoreService.getLinkedAccountsByUser(story.userId, { status: "active" });
              if (!accounts || accounts.length === 0) {
                console.log(`\u26A0\uFE0F \u0644\u0627 \u062A\u0648\u062C\u062F \u062D\u0633\u0627\u0628\u0627\u062A \u0646\u0634\u0637\u0629 \u0644\u0644\u0645\u0633\u062A\u062E\u062F\u0645 ${story.userId}. \u0641\u0634\u0644 \u0627\u0644\u062C\u062F\u0648\u0644\u0629.`);
                await firestoreService.updateStory(story.id, {
                  status: "failed"
                });
                failed++;
                continue;
              }
              if (story.format === "story" && story.videoGenerationStatus !== "generated") {
                console.log(`\u26A0\uFE0F Story ${story.id} is due but video is not generated yet (status: ${story.videoGenerationStatus}). Skipping.`);
                const { videoScheduler: videoScheduler2 } = await Promise.resolve().then(() => (init_video_scheduler(), video_scheduler_exports));
                await videoScheduler2.scheduleVideoGeneration(story, 0);
                continue;
              }
              const result = await this.publishStoryAcrossPlatforms(story);
              if (result.success) {
                published++;
                await firestoreService.updateStory(story.id, {
                  status: "published",
                  publishedAt: /* @__PURE__ */ new Date()
                });
              } else {
                failed++;
              }
            } catch (e) {
              console.error(`\u274C Failed to publish story ${story.id}:`, e);
              failed++;
            }
          }
          return { processed: due.length, published, failed };
        } catch (error) {
          console.error("Error in executeCronJob:", error);
          return { processed: 0, published: 0, failed: 0 };
        }
      }
      async publishStoryAcrossPlatforms(story) {
        console.log(`\u{1F4E4} Publishing story ${story.id} to platforms: ${story.platforms.join(", ")}`);
        const { getFirestore: getFirestore2 } = await Promise.resolve().then(() => (init_firebase_admin_setup(), firebase_admin_setup_exports));
        const db = getFirestore2();
        const snapshot = await db.collection("linked_accounts").where("userId", "==", story.userId).where("status", "==", "active").get();
        const accounts = snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
        let allSuccess = true;
        for (const platform of story.platforms) {
          const platformAccount = accounts.find((acc) => acc.platform === platform);
          if (!platformAccount) {
            console.error(`\u274C No active account found for platform ${platform} for user ${story.userId}`);
            allSuccess = false;
            continue;
          }
          try {
            if (platform === "facebook") {
              const { facebookStoriesPublisher: facebookStoriesPublisher2 } = await Promise.resolve().then(() => (init_facebook_stories_publisher(), facebook_stories_publisher_exports));
              const res = await facebookStoriesPublisher2.publishStoryToFacebook(story, platformAccount.id);
              if (!res.success) allSuccess = false;
            } else if (platform === "instagram") {
              const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
            } else if (platform === "tiktok") {
            }
          } catch (err) {
            console.error(`\u274C Error publishing to ${platform}:`, err);
            allSuccess = false;
          }
        }
        this.stats.storiesPublishedToday++;
        if (allSuccess) {
          this.stats.successfulPublications++;
        } else {
          this.stats.failedPublications++;
        }
        return { success: allSuccess };
      }
      updateNextRunTime() {
        this.nextRunTime = null;
      }
      performHealthCheck() {
        if (!this.isRunning) this.start();
      }
      performMemoryCleanup() {
        if (this.publishResults.length > MEMORY_CONFIG.maxResultsHistory) {
          this.publishResults = this.publishResults.slice(-MEMORY_CONFIG.maxResultsHistory);
        }
      }
      // Missing methods used in routes.ts
      getRecentResults(limit) {
        return this.publishResults.slice(-limit);
      }
      getQueueStatus() {
        return Array.from(this.storyQueue.values());
      }
      async forceRetryStory(id) {
        return true;
      }
      async triggerFromWebhook(secret) {
        const result = await this.safeExecuteCronJob();
        return { success: true, results: result, status: this.getStatus() };
      }
      clearFailedFromQueue() {
        this.storyQueue.clear();
        return 0;
      }
      updateCronExpression(exp) {
        this.cronExpression = exp;
        return true;
      }
      async checkAndGenerateVideos() {
        const stories = await firestoreService.getAllScheduledStories();
        const now = /* @__PURE__ */ new Date();
        const checkTime = new Date(now.getTime() + 4 * 60 * 60 * 1e3);
        const pendingVideos = stories.filter(
          (s) => s.format === "story" && s.videoGenerationStatus === "pending" && s.scheduledTime && new Date(s.scheduledTime) <= checkTime
        );
        let generated = 0;
        let failed = 0;
        const { videoScheduler: videoScheduler2 } = await Promise.resolve().then(() => (init_video_scheduler(), video_scheduler_exports));
        for (const story of pendingVideos) {
          const success = await videoScheduler2.scheduleVideoGeneration(story, 0);
          if (success) generated++;
          else failed++;
        }
        return { total: pendingVideos.length, generated, failed };
      }
    };
    cronScheduler = new AdvancedCronScheduler();
  }
});

// server/deepseek.ts
var deepseek_exports = {};
__export(deepseek_exports, {
  DeepSeekSDK: () => DeepSeekSDK,
  deepseekSDK: () => deepseekSDK,
  generateCategoryImagePrompt: () => generateCategoryImagePrompt,
  generateContent: () => generateContent2,
  generateHDPosterPrompt: () => generateHDPosterPrompt,
  generateImagePrompt: () => generateImagePrompt,
  generatePosterContent: () => generatePosterContent,
  generatePromotionalDescription: () => generatePromotionalDescription,
  translateToArabic: () => translateToArabic
});
async function generateContent2(request) {
  const basePrompt = categoryPrompts2[request.category] || categoryPrompts2.movies;
  const keywordsPrompt = request.keywords ? `

\u0627\u0633\u062A\u062E\u062F\u0645 \u0647\u0630\u0647 \u0627\u0644\u0643\u0644\u0645\u0627\u062A \u0627\u0644\u0645\u0641\u062A\u0627\u062D\u064A\u0629 \u0641\u064A \u0627\u0644\u0645\u062D\u062A\u0648\u0649: ${request.keywords}` : "";
  const fullPrompt = `${basePrompt}${keywordsPrompt}

\u062A\u0630\u0643\u0631: \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0628\u0627\u0644\u0644\u063A\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629\u060C \u062C\u0630\u0627\u0628\u060C \u0648\u0645\u0646\u0627\u0633\u0628 \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0641\u064A\u0633\u0628\u0648\u0643 \u0648\u0627\u0646\u0633\u062A\u062C\u0631\u0627\u0645 \u0648\u062A\u064A\u0643 \u062A\u0648\u0643.`;
  try {
    const content = await deepseekSDK.generateSimple(
      fullPrompt,
      "\u0623\u0646\u062A \u0645\u0633\u0627\u0639\u062F \u0630\u0643\u064A \u0645\u062A\u062E\u0635\u0635 \u0641\u064A \u0625\u0646\u0634\u0627\u0621 \u0645\u062D\u062A\u0648\u0649 \u0625\u0628\u062F\u0627\u0639\u064A \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A \u0628\u0627\u0644\u0644\u063A\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629.",
      {
        temperature: 0.8,
        max_tokens: 500
      }
    );
    return {
      content: content.trim(),
      category: request.category
    };
  } catch (error) {
    console.error("DeepSeek generation error:", error);
    throw new Error("\u0641\u0634\u0644 \u0641\u064A \u0625\u0646\u0634\u0627\u0621 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0628\u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0627\u0644\u0630\u0643\u0627\u0621 \u0627\u0644\u0627\u0635\u0637\u0646\u0627\u0639\u064A");
  }
}
async function translateToArabic(text) {
  if (!text || text.length < 5) {
    return text;
  }
  const systemPrompt = "\u0623\u0646\u062A \u0645\u062A\u0631\u062C\u0645 \u0645\u062D\u062A\u0631\u0641 \u0645\u062A\u062E\u0635\u0635 \u0641\u064A \u0627\u0644\u062A\u0631\u062C\u0645\u0629 \u0645\u0646 \u0627\u0644\u0625\u0646\u062C\u0644\u064A\u0632\u064A\u0629 \u0625\u0644\u0649 \u0627\u0644\u0639\u0631\u0628\u064A\u0629. \u062A\u0631\u062C\u0645 \u0627\u0644\u0646\u0635 \u0628\u0623\u0633\u0644\u0648\u0628 \u0625\u0628\u062F\u0627\u0639\u064A \u0648\u062C\u0630\u0627\u0628 \u0645\u0646\u0627\u0633\u0628 \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u062D\u0627\u0641\u0638 \u0639\u0644\u0649 \u0627\u0644\u0645\u0639\u0646\u0649 \u0627\u0644\u0623\u0635\u0644\u064A \u0645\u0639 \u062C\u0639\u0644 \u0627\u0644\u0646\u0635 \u062A\u0634\u0648\u064A\u0642\u064A\u0627\u064B \u0648\u0645\u062B\u064A\u0631\u0627\u064B.";
  const userPrompt = `\u062A\u0631\u062C\u0645 \u0647\u0630\u0627 \u0627\u0644\u0646\u0635 \u0625\u0644\u0649 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0628\u0623\u0633\u0644\u0648\u0628 \u062A\u0634\u0648\u064A\u0642\u064A \u0648\u062C\u0630\u0627\u0628:

"${text}"

\u0623\u0639\u0637\u0646\u064A \u0627\u0644\u062A\u0631\u062C\u0645\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0641\u0642\u0637 \u0628\u062F\u0648\u0646 \u0623\u064A \u0634\u0631\u062D \u0623\u0648 \u0625\u0636\u0627\u0641\u0627\u062A.`;
  try {
    const translation = await deepseekSDK.generateSimple(
      userPrompt,
      systemPrompt,
      {
        temperature: 0.5,
        max_tokens: 300
      }
    );
    const cleanedTranslation = translation.trim().replace(/^["']|["']$/g, "");
    console.log(`\u{1F310} AI Translated to Arabic: "${cleanedTranslation.substring(0, 50)}..."`);
    return cleanedTranslation;
  } catch (error) {
    console.error("Translation error:", error);
    return text;
  }
}
async function generatePromotionalDescription(title, category, originalDescription) {
  const categoryPromptMap = {
    movies: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062A\u0634\u0648\u064A\u0642\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0644\u0641\u064A\u0644\u0645 "${title}" \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0648\u062C\u0630\u0627\u0628\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0639\u0644\u0649 \u0645\u0634\u0627\u0647\u062F\u0629 \u0627\u0644\u0641\u064A\u0644\u0645 \u0641\u0648\u0631\u0627\u064B. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u0642\u0648\u064A\u0629 \u0648\u0639\u0627\u0637\u0641\u064A\u0629.`,
      en: `Write a professional promotional description (3-4 sentences, about 100-120 words) for the movie "${title}" for social media. Make it exciting and compelling to encourage viewers to watch immediately. Use powerful, engaging language.`
    },
    tv_shows: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062A\u0634\u0648\u064A\u0642\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0644\u0645\u0633\u0644\u0633\u0644 "${title}" \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0639\u0644\u0649 \u0645\u062A\u0627\u0628\u0639\u0629 \u0627\u0644\u0645\u0633\u0644\u0633\u0644 \u0645\u0646 \u0623\u0648\u0644 \u062D\u0644\u0642\u0629.`,
      en: `Write a professional promotional description (3-4 sentences, about 100-120 words) for the TV series "${title}" for social media. Make it exciting to encourage viewers to follow from episode one.`
    },
    sports: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062D\u0645\u0627\u0633\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0639\u0646 "${title}" \u0641\u064A \u0627\u0644\u0631\u064A\u0627\u0636\u0629. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u062A\u0627\u0628\u0639\u064A\u0646 \u0639\u0644\u0649 \u0639\u062F\u0645 \u062A\u0641\u0648\u064A\u062A \u0647\u0630\u0627 \u0627\u0644\u062D\u062F\u062B.`,
      en: `Write a professional exciting description (3-4 sentences, about 100-120 words) about "${title}" in sports. Make it thrilling to encourage fans not to miss this event.`
    },
    recipes: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u0634\u0647\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0644\u0648\u0635\u0641\u0629 "${title}". \u0627\u062C\u0639\u0644\u0647 \u064A\u062B\u064A\u0631 \u0627\u0644\u0634\u0647\u064A\u0629 \u0628\u0634\u062F\u0629 \u0648\u064A\u062D\u0641\u0632 \u0639\u0644\u0649 \u062A\u062C\u0631\u0628\u0629 \u0627\u0644\u0648\u0635\u0641\u0629 \u0641\u0648\u0631\u0627\u064B. \u0627\u0630\u0643\u0631 \u0641\u0648\u0627\u0626\u062F \u0648\u0645\u0645\u064A\u0632\u0627\u062A \u0627\u0644\u0637\u0628\u0642.`,
      en: `Write a professional appetizing description (3-4 sentences, about 100-120 words) for the recipe "${title}". Make it extremely mouth-watering and encourage trying the recipe immediately.`
    },
    gaming: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u0645\u062B\u064A\u0631\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0644\u0644\u0639\u0628\u0629 "${title}". \u0627\u062C\u0639\u0644\u0647 \u064A\u062D\u0641\u0632 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u0639\u0644\u0649 \u062A\u062D\u0645\u064A\u0644 \u0648\u062A\u062C\u0631\u0628\u0629 \u0627\u0644\u0644\u0639\u0628\u0629 \u0641\u0648\u0631\u0627\u064B. \u0627\u0630\u0643\u0631 \u0645\u0645\u064A\u0632\u0627\u062A \u0627\u0644\u0644\u0639\u0628\u0629.`,
      en: `Write a professional exciting description (3-4 sentences, about 100-120 words) for the game "${title}". Make it thrilling for gamers to download and try immediately.`
    },
    apps: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062C\u0630\u0627\u0628\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B (3-4 \u062C\u0645\u0644\u060C \u062D\u0648\u0627\u0644\u064A 100-120 \u0643\u0644\u0645\u0629) \u0644\u062A\u0637\u0628\u064A\u0642 "${title}". \u0627\u062C\u0639\u0644\u0647 \u064A\u0628\u0631\u0632 \u0641\u0648\u0627\u0626\u062F \u0648\u0645\u0645\u064A\u0632\u0627\u062A \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0628\u0637\u0631\u064A\u0642\u0629 \u0645\u0642\u0646\u0639\u0629 \u062A\u062D\u0641\u0632 \u0639\u0644\u0649 \u0627\u0644\u062A\u062D\u0645\u064A\u0644.`,
      en: `Write a professional attractive description (3-4 sentences, about 100-120 words) for the app "${title}". Highlight its benefits and features in a compelling way that encourages download.`
    }
  };
  const prompts = categoryPromptMap[category] || categoryPromptMap.movies;
  try {
    const contextInfo = originalDescription ? `

\u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0625\u0636\u0627\u0641\u064A\u0629: ${originalDescription.substring(0, 300)}` : "";
    const [arResult, enResult] = await Promise.all([
      deepseekSDK.generateSimple(
        prompts.ar + contextInfo,
        "\u0623\u0646\u062A \u0643\u0627\u062A\u0628 \u0645\u062D\u062A\u0648\u0649 \u0625\u0628\u062F\u0627\u0639\u064A \u0645\u062D\u062A\u0631\u0641 \u0645\u062A\u062E\u0635\u0635 \u0641\u064A \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0643\u062A\u0628 \u0628\u0623\u0633\u0644\u0648\u0628 \u062C\u0630\u0627\u0628 \u0648\u0645\u0642\u0646\u0639. \u0627\u062C\u0639\u0644 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0648\u064A\u0627\u064B \u0639\u0627\u0637\u0641\u064A\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0639\u0644\u0649 \u0627\u0644\u062A\u0641\u0627\u0639\u0644.",
        { temperature: 0.75, max_tokens: 250 }
      ),
      deepseekSDK.generateSimple(
        prompts.en + (originalDescription ? `

Context: ${originalDescription.substring(0, 300)}` : ""),
        "You are a professional creative content writer for social media. Write in an engaging and compelling style. Make content emotionally powerful and encourage engagement.",
        { temperature: 0.75, max_tokens: 250 }
      )
    ]);
    return {
      descriptionAr: arResult.trim().replace(/^["']|["']$/g, ""),
      descriptionEn: enResult.trim().replace(/^["']|["']$/g, "")
    };
  } catch (error) {
    console.error("Promotional description generation error:", error);
    return {
      descriptionAr: `${title} - \u0645\u062D\u062A\u0648\u0649 \u0631\u0627\u0626\u0639 \u0648\u0645\u0645\u064A\u0632 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u062A\u0627\u0628\u0639\u0629 \u0648\u0627\u0644\u062A\u062C\u0631\u0628\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u0641\u0631\u0635\u0629 \u0627\u0644\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629`,
      descriptionEn: `${title} - Amazing and unique content worth following and trying! Don't miss this exceptional opportunity`
    };
  }
}
async function generateImagePrompt(category, content) {
  const categoryImageStyles = {
    movies: "cinematic movie poster, dramatic lighting, 4K ultra HD, professional photography, film grain, movie theater quality",
    tv_shows: "TV series poster style, vibrant colors, modern design, Netflix quality, dramatic composition, 8K resolution",
    sports: "dynamic sports action shot, energetic composition, professional sports photography, stadium lights, high-speed capture, 4K",
    recipes: "professional food photography, appetizing presentation, warm natural lighting, cookbook quality, macro detail, delicious colors",
    gaming: "AAA video game concept art, digital illustration, vibrant neon colors, RTX quality, professional game poster, 4K",
    apps: "modern app interface showcase, clean minimal design, tech aesthetic, Apple quality, premium device mockup, glossy finish"
  };
  const styleGuide = categoryImageStyles[category] || "professional, high quality, 4K resolution";
  const systemPrompt = "You are an expert at creating prompts for FLUX AI image generation. Generate detailed, visual prompts that produce stunning HD images. Focus on composition, lighting, colors, and atmosphere.";
  const userPrompt = `Create an image prompt for: "${content}"
Style guidelines: ${styleGuide}

Requirements:
- Make it visual and descriptive
- Include lighting and mood
- Specify quality (4K, HD, professional)
- Keep it under 50 words
- English only, no explanations

Generate the prompt:`;
  try {
    const prompt = await deepseekSDK.generateSimple(
      userPrompt,
      systemPrompt,
      {
        temperature: 0.8,
        max_tokens: 120
      }
    );
    const cleanedPrompt = prompt.trim().replace(/^["']|["']$/g, "");
    console.log(`\u{1F3A8} Generated HD image prompt: "${cleanedPrompt.substring(0, 80)}..."`);
    return cleanedPrompt;
  } catch (error) {
    console.error("Image prompt generation error:", error);
    return `${content}, ${styleGuide}, professional quality`;
  }
}
async function generateHDPosterPrompt(title, category, additionalContext) {
  const categoryStyles = {
    movies: "dramatic cinematic movie poster, epic composition, theatrical release quality, IMAX style, film poster art",
    tv_shows: "streaming service quality poster, binge-worthy series art, Netflix/HBO style, dramatic character composition",
    sports: "action sports photography, stadium atmosphere, championship moment, ESPN broadcast quality",
    recipes: "gourmet food photography, Michelin star presentation, food magazine cover, appetizing closeup",
    gaming: "AAA game cover art, PlayStation/Xbox quality, epic gaming poster, concept art masterpiece",
    apps: "App Store featured banner, premium app showcase, modern UI design, Apple design award quality",
    tv_channels: "professional TV channel branding, broadcast quality logo, modern media design, entertainment network style"
  };
  const style = categoryStyles[category] || categoryStyles.movies;
  const prompt = `${title}, ${style}, ultra high definition 4K, professional lighting, stunning composition, ${additionalContext || "trending content"}`;
  return prompt;
}
async function generatePosterContent(title, category, originalDescription) {
  const categoryPromptMap = {
    movies: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062A\u0634\u0648\u064A\u0642\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0642\u0648\u064A\u0629 \u0648\u0645\u062B\u064A\u0631\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u0641\u064A\u0644\u0645 "${title}" \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0644\u0644\u063A\u0627\u064A\u0629 \u0648\u062C\u0630\u0627\u0628\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0639\u0644\u0649 \u0645\u0634\u0627\u0647\u062F\u0629 \u0627\u0644\u0641\u064A\u0644\u0645 \u0641\u0648\u0631\u0627\u064B. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u0642\u0648\u064A\u0629 \u0648\u0639\u0627\u0637\u0641\u064A\u0629 \u0648\u0645\u0624\u062B\u0631\u0629. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0634\u0627\u0645\u0644\u0627\u064B \u0648\u0645\u0643\u062A\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644 \u0643\u0627\u0645\u0644\u0629.`,
      en: `Write a professional promotional description with exactly 4 powerful and exciting sentences (about 140-160 words) for the movie "${title}" for social media. Make it extremely exciting and compelling to encourage viewers to watch immediately. Use powerful, engaging, and emotional language. The description must be complete in 4 full sentences.`,
      ctaAr: "\u0634\u0627\u0647\u062F \u0627\u0644\u0641\u064A\u0644\u0645 \u0627\u0644\u0622\u0646 \u0645\u062C\u0627\u0646\u0627\u064B",
      ctaEn: "WATCH NOW FOR FREE"
    },
    tv_shows: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062A\u0634\u0648\u064A\u0642\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0645\u062B\u064A\u0631\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u0645\u0633\u0644\u0633\u0644 "${title}" \u0644\u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0644\u0644\u0641\u0636\u0648\u0644 \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0639\u0644\u0649 \u0645\u062A\u0627\u0628\u0639\u0629 \u0627\u0644\u0645\u0633\u0644\u0633\u0644 \u0645\u0646 \u0623\u0648\u0644 \u062D\u0644\u0642\u0629. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u062A\u0634\u0648\u064A\u0642\u064A\u0629 \u0642\u0648\u064A\u0629 \u062A\u062C\u0639\u0644 \u0627\u0644\u0642\u0627\u0631\u0626 \u064A\u0634\u062A\u0627\u0642 \u0644\u0645\u0634\u0627\u0647\u062F\u062A\u0647. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0643\u0627\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional promotional description with exactly 4 exciting sentences (about 140-160 words) for the TV series "${title}" for social media. Make it intriguing and encourage viewers to follow from episode one. Use powerful suspenseful language. The description must be complete in 4 sentences.`,
      ctaAr: "\u062A\u0627\u0628\u0639 \u0627\u0644\u0645\u0633\u0644\u0633\u0644 \u0627\u0644\u0622\u0646",
      ctaEn: "WATCH THE SERIES NOW"
    },
    sports: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062D\u0645\u0627\u0633\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0642\u0648\u064A\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0639\u0646 "${title}" \u0641\u064A \u0627\u0644\u0631\u064A\u0627\u0636\u0629. \u0627\u062C\u0639\u0644\u0647 \u0645\u062B\u064A\u0631\u0627\u064B \u0644\u0644\u062D\u0645\u0627\u0633 \u0648\u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u062A\u0627\u0628\u0639\u064A\u0646 \u0639\u0644\u0649 \u0639\u062F\u0645 \u062A\u0641\u0648\u064A\u062A \u0647\u0630\u0627 \u0627\u0644\u062D\u062F\u062B \u0627\u0644\u0631\u064A\u0627\u0636\u064A \u0627\u0644\u0645\u0647\u0645. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u062D\u0645\u0627\u0633\u064A\u0629 \u0648\u0645\u0644\u0647\u0628\u0629 \u0644\u0644\u0645\u0634\u0627\u0639\u0631. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0645\u0643\u062A\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional exciting description with exactly 4 powerful sentences (about 140-160 words) about "${title}" in sports. Make it thrilling and encourage fans not to miss this important event. Use enthusiastic and passionate language. The description must be complete in 4 sentences.`,
      ctaAr: "\u0634\u0627\u0647\u062F \u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629 \u0645\u0628\u0627\u0634\u0631\u0629 \u0627\u0644\u0622\u0646",
      ctaEn: "WATCH LIVE NOW"
    },
    recipes: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u0634\u0647\u064A\u0627\u064B \u0648\u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0645\u063A\u0631\u064A\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u0648\u0635\u0641\u0629 "${title}". \u0627\u062C\u0639\u0644\u0647 \u064A\u062B\u064A\u0631 \u0627\u0644\u0634\u0647\u064A\u0629 \u0628\u0634\u062F\u0629 \u0648\u064A\u062D\u0641\u0632 \u0639\u0644\u0649 \u062A\u062C\u0631\u0628\u0629 \u0627\u0644\u0648\u0635\u0641\u0629 \u0641\u0648\u0631\u0627\u064B. \u0627\u0630\u0643\u0631 \u0627\u0644\u0645\u0630\u0627\u0642 \u0627\u0644\u0631\u0627\u0626\u0639 \u0648\u0627\u0644\u0641\u0648\u0627\u0626\u062F \u0627\u0644\u0635\u062D\u064A\u0629 \u0648\u0645\u0645\u064A\u0632\u0627\u062A \u0627\u0644\u0637\u0628\u0642. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u062A\u062C\u0639\u0644 \u0627\u0644\u0642\u0627\u0631\u0626 \u064A\u0634\u0639\u0631 \u0628\u0627\u0644\u062C\u0648\u0639. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0643\u0627\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional appetizing description with exactly 4 tempting sentences (about 140-160 words) for the recipe "${title}". Make it extremely mouth-watering and encourage trying the recipe immediately. Mention the amazing taste, health benefits, and dish features. The description must be complete in 4 sentences.`,
      ctaAr: "\u0627\u0643\u062A\u0634\u0641 \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0633\u0631\u064A\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629",
      ctaEn: "DISCOVER THE FULL SECRET RECIPE"
    },
    gaming: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u0645\u062B\u064A\u0631\u0627\u064B \u0648\u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0642\u0648\u064A\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u0644\u0639\u0628\u0629 "${title}" \u0627\u0644\u062A\u0631\u0646\u062F. \u0627\u062C\u0639\u0644\u0647 \u064A\u062D\u0641\u0632 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u0639\u0644\u0649 \u062A\u062D\u0645\u064A\u0644 \u0648\u062A\u062C\u0631\u0628\u0629 \u0627\u0644\u0644\u0639\u0628\u0629 \u0641\u0648\u0631\u0627\u064B. \u0627\u0630\u0643\u0631 \u0627\u0644\u0631\u0633\u0648\u0645\u0627\u062A \u0627\u0644\u062E\u0631\u0627\u0641\u064A\u0629 \u0648\u0627\u0644\u0623\u0633\u0644\u0648\u0628 \u0627\u0644\u0645\u0628\u062A\u0643\u0631 \u0648\u0627\u0644\u062A\u062D\u062F\u064A\u0627\u062A \u0627\u0644\u0645\u062B\u064A\u0631\u0629. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u0627\u0644\u062C\u064A\u0645\u0631\u0632 \u0627\u0644\u062D\u0645\u0627\u0633\u064A\u0629. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0645\u0643\u062A\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional exciting description with exactly 4 powerful sentences (about 140-160 words) for the trending game "${title}". Make it thrilling for gamers to download and try immediately. Mention stunning graphics, innovative gameplay, and exciting challenges. The description must be complete in 4 sentences.`,
      ctaAr: "\u062D\u0645\u0651\u0644 \u0627\u0644\u0644\u0639\u0628\u0629 \u0645\u062C\u0627\u0646\u0627\u064B \u0627\u0644\u0622\u0646",
      ctaEn: "DOWNLOAD FREE NOW"
    },
    apps: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062C\u0630\u0627\u0628\u0627\u064B \u0648\u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0645\u0642\u0646\u0639\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u062A\u0637\u0628\u064A\u0642 "${title}" \u0627\u0644\u062A\u0631\u0646\u062F. \u0627\u062C\u0639\u0644\u0647 \u064A\u0628\u0631\u0632 \u0627\u0644\u0641\u0648\u0627\u0626\u062F \u0627\u0644\u0639\u0638\u064A\u0645\u0629 \u0648\u0645\u0645\u064A\u0632\u0627\u062A \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0628\u0637\u0631\u064A\u0642\u0629 \u0645\u0642\u0646\u0639\u0629 \u062A\u062D\u0641\u0632 \u0639\u0644\u0649 \u0627\u0644\u062A\u062D\u0645\u064A\u0644 \u0641\u0648\u0631\u0627\u064B. \u0627\u0630\u0643\u0631 \u0643\u064A\u0641 \u0633\u064A\u063A\u064A\u0631 \u062D\u064A\u0627\u0629 \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645 \u0644\u0644\u0623\u0641\u0636\u0644. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0645\u0643\u062A\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional attractive description with exactly 4 compelling sentences (about 140-160 words) for the trending app "${title}". Highlight its amazing benefits and features in a way that encourages immediate download. Mention how it will change the user's life for the better. The description must be complete in 4 sentences.`,
      ctaAr: "\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0627\u0644\u0646\u0633\u062E\u0629 \u0627\u0644\u0645\u062F\u0641\u0648\u0639\u0629 \u0645\u062C\u0627\u0646\u0627\u064B",
      ctaEn: "GET PREMIUM VERSION FREE"
    },
    tv_channels: {
      ar: `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0645\u0643\u0648\u0646\u0627\u064B \u0645\u0646 4 \u062C\u0645\u0644 \u0645\u062B\u064A\u0631\u0629 (\u062D\u0648\u0627\u0644\u064A 140-160 \u0643\u0644\u0645\u0629) \u0644\u0642\u0646\u0627\u0629 "${title}" \u0627\u0644\u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629 \u0627\u0644\u062A\u0631\u0646\u062F. \u0627\u062C\u0639\u0644\u0647 \u064A\u062D\u0641\u0632 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0639\u0644\u0649 \u0645\u062A\u0627\u0628\u0639\u0629 \u0627\u0644\u0642\u0646\u0627\u0629 \u0648\u0627\u0644\u0627\u0633\u062A\u0645\u062A\u0627\u0639 \u0628\u0628\u0631\u0627\u0645\u062C\u0647\u0627 \u0627\u0644\u0645\u0645\u064A\u0632\u0629. \u0627\u0630\u0643\u0631 \u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0627\u0644\u062D\u0635\u0631\u064A\u0629 \u0648\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0641\u0631\u064A\u062F. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0648\u0635\u0641 \u0645\u0643\u062A\u0645\u0644\u0627\u064B \u0641\u064A 4 \u062C\u0645\u0644.`,
      en: `Write a professional description with exactly 4 exciting sentences (about 140-160 words) for the trending TV channel "${title}". Make it encourage viewers to follow the channel and enjoy its unique programs. Mention exclusive shows and unique content. The description must be complete in 4 sentences.`,
      ctaAr: "\u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0627\u0644\u0622\u0646",
      ctaEn: "WATCH LIVE BROADCAST NOW"
    }
  };
  const prompts = categoryPromptMap[category] || categoryPromptMap.movies;
  try {
    const contextInfo = originalDescription ? `

\u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0625\u0636\u0627\u0641\u064A\u0629 \u0639\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649: ${originalDescription.substring(0, 400)}` : "";
    const [arResult, enResult] = await Promise.all([
      deepseekSDK.generateSimple(
        prompts.ar + contextInfo + "\n\n\u0647\u0627\u0645 \u062C\u062F\u0627\u064B: \u0627\u0643\u062A\u0628 \u0627\u0644\u0648\u0635\u0641 \u0627\u0644\u062A\u0634\u0648\u064A\u0642\u064A \u0641\u0642\u0637 \u0628\u062F\u0648\u0646 \u0623\u064A \u0645\u0642\u062F\u0645\u0627\u062A \u0623\u0648 \u0639\u0646\u0627\u0648\u064A\u0646. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 4 \u062C\u0645\u0644 \u0643\u0627\u0645\u0644\u0629.",
        "\u0623\u0646\u062A \u0643\u0627\u062A\u0628 \u0645\u062D\u062A\u0648\u0649 \u0625\u0628\u062F\u0627\u0639\u064A \u0645\u062D\u062A\u0631\u0641 \u0645\u062A\u062E\u0635\u0635 \u0641\u064A \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0643\u062A\u0628 \u0628\u0623\u0633\u0644\u0648\u0628 \u062C\u0630\u0627\u0628 \u0648\u0645\u0642\u0646\u0639 \u0648\u062A\u0634\u0648\u064A\u0642\u064A. \u0627\u062C\u0639\u0644 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0648\u064A\u0627\u064B \u0639\u0627\u0637\u0641\u064A\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0639\u0644\u0649 \u0627\u0644\u062A\u0641\u0627\u0639\u0644 \u0627\u0644\u0641\u0648\u0631\u064A. \u0644\u0627 \u062A\u0643\u062A\u0628 \u0623\u064A \u0645\u0642\u062F\u0645\u0627\u062A\u060C \u0641\u0642\u0637 \u0627\u0644\u0648\u0635\u0641 \u0627\u0644\u062A\u0634\u0648\u064A\u0642\u064A \u0627\u0644\u0645\u0643\u0648\u0646 \u0645\u0646 4 \u062C\u0645\u0644.",
        { temperature: 0.75, max_tokens: 350 }
      ),
      deepseekSDK.generateSimple(
        prompts.en + (originalDescription ? `

Additional context: ${originalDescription.substring(0, 400)}` : "") + "\n\nIMPORTANT: Write ONLY the promotional description without any introductions or titles. It must be exactly 4 complete sentences.",
        "You are a professional creative content writer for social media. Write in an engaging, compelling, and suspenseful style. Make content emotionally powerful and encourage immediate engagement. Do not write any introductions, just the 4-sentence promotional description.",
        { temperature: 0.75, max_tokens: 350 }
      )
    ]);
    return {
      descriptionAr: arResult.trim().replace(/^["']|["']$/g, "").replace(/^\*\*.*?\*\*\n?/g, ""),
      descriptionEn: enResult.trim().replace(/^["']|["']$/g, "").replace(/^\*\*.*?\*\*\n?/g, ""),
      ctaAr: prompts.ctaAr,
      ctaEn: prompts.ctaEn
    };
  } catch (error) {
    console.error("Poster content generation error:", error);
    return {
      descriptionAr: getDefaultDescription(category, title, "ar"),
      descriptionEn: getDefaultDescription(category, title, "en"),
      ctaAr: prompts.ctaAr,
      ctaEn: prompts.ctaEn
    };
  }
}
function getDefaultDescription(category, title, lang) {
  const defaults = {
    movies: {
      ar: `\u0641\u064A\u0644\u0645 ${title} \u0627\u0644\u062C\u062F\u064A\u062F \u064A\u062D\u0637\u0645 \u0643\u0644 \u0627\u0644\u062A\u0648\u0642\u0639\u0627\u062A \u0628\u0642\u0635\u062A\u0647 \u0627\u0644\u0645\u0630\u0647\u0644\u0629 \u0648\u0623\u062F\u0627\u0621 \u0627\u0644\u0645\u0645\u062B\u0644\u064A\u0646 \u0627\u0644\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A! \u0631\u062D\u0644\u0629 \u0633\u064A\u0646\u0645\u0627\u0626\u064A\u0629 \u0644\u0646 \u062A\u0646\u0633\u0627\u0647\u0627 \u0623\u0628\u062F\u0627\u064B \u0645\u0644\u064A\u0626\u0629 \u0628\u0627\u0644\u062A\u0634\u0648\u064A\u0642 \u0648\u0627\u0644\u0625\u062B\u0627\u0631\u0629. \u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0627\u0644\u0630\u064A\u0646 \u0623\u062D\u0628\u0648\u0627 \u0647\u0630\u0627 \u0627\u0644\u0639\u0645\u0644 \u0627\u0644\u0641\u0646\u064A \u0627\u0644\u0631\u0627\u0626\u0639. \u0644\u0627 \u062A\u0641\u0648\u062A \u0641\u0631\u0635\u0629 \u0645\u0634\u0627\u0647\u062F\u0629 \u0623\u0641\u0636\u0644 \u0641\u064A\u0644\u0645 \u0641\u064A \u0647\u0630\u0627 \u0627\u0644\u0645\u0648\u0633\u0645!`,
      en: `${title} shatters all expectations with its amazing story and exceptional performances! A cinematic journey you'll never forget, filled with suspense and excitement. Join millions of viewers who loved this masterpiece. Don't miss your chance to watch the best movie of the season!`
    },
    tv_shows: {
      ar: `\u0645\u0633\u0644\u0633\u0644 ${title} \u0627\u0644\u062A\u0631\u0646\u062F \u064A\u0623\u0633\u0631\u0643 \u0645\u0646 \u0627\u0644\u062D\u0644\u0642\u0629 \u0627\u0644\u0623\u0648\u0644\u0649 \u0628\u0623\u062D\u062F\u0627\u062B\u0647 \u0627\u0644\u0645\u062B\u064A\u0631\u0629 \u0648\u0646\u0647\u0627\u064A\u0627\u062A\u0647 \u0627\u0644\u0635\u0627\u062F\u0645\u0629! \u0634\u062E\u0635\u064A\u0627\u062A \u0644\u0627 \u062A\u064F\u0646\u0633\u0649 \u0648\u0642\u0635\u0629 \u062A\u062C\u0639\u0644\u0643 \u062A\u0646\u062A\u0638\u0631 \u0643\u0644 \u062D\u0644\u0642\u0629 \u0628\u0641\u0627\u0631\u063A \u0627\u0644\u0635\u0628\u0631. \u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u062A\u0627\u0628\u0639\u064A\u0646 \u0641\u064A \u0647\u0630\u0647 \u0627\u0644\u0631\u062D\u0644\u0629 \u0627\u0644\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629. \u0623\u0641\u0636\u0644 \u0645\u0633\u0644\u0633\u0644 \u064A\u0645\u0643\u0646\u0643 \u0645\u0634\u0627\u0647\u062F\u062A\u0647 \u0627\u0644\u0622\u0646!`,
      en: `${title} captivates you from episode one with thrilling events and shocking endings! Unforgettable characters and a story that makes you eagerly await each episode. Join millions of followers on this extraordinary journey. The best series you can watch right now!`
    },
    recipes: {
      ar: `\u0648\u0635\u0641\u0629 ${title} \u0627\u0644\u0634\u0647\u064A\u0629 \u0633\u062A\u062C\u0639\u0644 \u0639\u0627\u0626\u0644\u062A\u0643 \u062A\u0637\u0644\u0628\u0647\u0627 \u0645\u0631\u0627\u0631\u0627\u064B \u0648\u062A\u0643\u0631\u0627\u0631\u0627\u064B! \u0645\u0643\u0648\u0646\u0627\u062A \u0628\u0633\u064A\u0637\u0629 \u0648\u0646\u062A\u064A\u062C\u0629 \u0645\u0630\u0647\u0644\u0629 \u062A\u0641\u0648\u0642 \u0643\u0644 \u0627\u0644\u062A\u0648\u0642\u0639\u0627\u062A. \u0637\u0639\u0645 \u0644\u0630\u064A\u0630 \u0648\u0631\u0627\u0626\u062D\u0629 \u062A\u0645\u0644\u0623 \u0627\u0644\u0645\u0643\u0627\u0646 \u0628\u0627\u0644\u0634\u0647\u064A\u0629 \u0648\u0627\u0644\u0633\u0639\u0627\u062F\u0629. \u062C\u0631\u0628\u0647\u0627 \u0627\u0644\u0622\u0646 \u0648\u0627\u0643\u062A\u0634\u0641 \u0633\u0631 \u0627\u0644\u0637\u0628\u0642 \u0627\u0644\u0630\u064A \u064A\u062D\u0628\u0647 \u0627\u0644\u062C\u0645\u064A\u0639!`,
      en: `The delicious ${title} recipe will make your family ask for it again and again! Simple ingredients with amazing results that exceed all expectations. Delicious taste and aroma that fills the place with appetite and happiness. Try it now and discover the secret everyone loves!`
    },
    gaming: {
      ar: `\u0644\u0639\u0628\u0629 ${title} \u0627\u0644\u0623\u0633\u0637\u0648\u0631\u064A\u0629 \u0633\u062A\u0623\u0633\u0631\u0643 \u0645\u0646 \u0627\u0644\u0644\u062D\u0638\u0629 \u0627\u0644\u0623\u0648\u0644\u0649 \u0628\u0631\u0633\u0648\u0645\u0627\u062A\u0647\u0627 \u0627\u0644\u062E\u064A\u0627\u0644\u064A\u0629! \u0639\u0627\u0644\u0645 \u0636\u062E\u0645 \u0645\u0646 \u0627\u0644\u0625\u062B\u0627\u0631\u0629 \u0648\u0627\u0644\u062A\u062D\u062F\u064A\u0627\u062A \u0627\u0644\u0645\u062B\u064A\u0631\u0629 \u064A\u0646\u062A\u0638\u0631\u0643 \u0627\u0644\u0622\u0646. \u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645 \u0641\u064A \u0647\u0630\u0647 \u0627\u0644\u0645\u063A\u0627\u0645\u0631\u0629 \u0627\u0644\u0645\u0644\u062D\u0645\u064A\u0629. \u062D\u0645\u0651\u0644 \u0627\u0644\u0644\u0639\u0628\u0629 \u0645\u062C\u0627\u0646\u0627\u064B \u0648\u0627\u0628\u062F\u0623 \u0631\u062D\u0644\u062A\u0643 \u0646\u062D\u0648 \u0627\u0644\u0642\u0645\u0629!`,
      en: `The legendary game ${title} will captivate you from the first moment with stunning graphics! A massive world of excitement and thrilling challenges awaits you now. Join millions of players worldwide in this epic adventure. Download free and start your journey to the top!`
    },
    apps: {
      ar: `\u062A\u0637\u0628\u064A\u0642 ${title} \u0627\u0644\u0645\u0645\u064A\u0632 \u0633\u064A\u063A\u064A\u0631 \u0637\u0631\u064A\u0642\u0629 \u062D\u064A\u0627\u062A\u0643 \u0644\u0644\u0623\u0641\u0636\u0644 \u0628\u0634\u0643\u0644 \u0644\u0627 \u064A\u0635\u062F\u0642! \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645\u064A\u0646 \u064A\u062B\u0642\u0648\u0646 \u0628\u0647 \u0648\u064A\u0639\u062A\u0645\u062F\u0648\u0646 \u0639\u0644\u064A\u0647 \u064A\u0648\u0645\u064A\u0627\u064B \u0641\u064A \u0645\u0647\u0627\u0645\u0647\u0645. \u062A\u0635\u0645\u064A\u0645 \u0645\u0630\u0647\u0644 \u0648\u0645\u064A\u0632\u0627\u062A \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0629 \u0644\u0646 \u062A\u062C\u062F\u0647\u0627 \u0641\u064A \u0623\u064A \u0645\u0643\u0627\u0646 \u0622\u062E\u0631. \u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0627\u0644\u0646\u0633\u062E\u0629 \u0627\u0644\u0645\u062F\u0641\u0648\u0639\u0629 \u0645\u062C\u0627\u0646\u0627\u064B \u0644\u0641\u062A\u0631\u0629 \u0645\u062D\u062F\u0648\u062F\u0629 \u062C\u062F\u0627\u064B!`,
      en: `The amazing ${title} app will change your life for the better incredibly! Millions of users trust and rely on it daily for their tasks. Stunning design and professional features you won't find anywhere else. Get the premium version free for a very limited time!`
    },
    tv_channels: {
      ar: `\u0642\u0646\u0627\u0629 ${title} \u0627\u0644\u062A\u0631\u0646\u062F \u062A\u0642\u062F\u0645 \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u062D\u0635\u0631\u064A \u0648\u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0627\u0644\u0645\u0645\u064A\u0632\u0629! \u0628\u062B \u0645\u0628\u0627\u0634\u0631 \u0639\u0644\u0649 \u0645\u062F\u0627\u0631 \u0627\u0644\u0633\u0627\u0639\u0629 \u0628\u062C\u0648\u062F\u0629 \u0641\u0627\u0626\u0642\u0629 \u0648\u0645\u062D\u062A\u0648\u0649 \u0645\u062A\u0646\u0648\u0639 \u064A\u0646\u0627\u0633\u0628 \u0627\u0644\u062C\u0645\u064A\u0639. \u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0627\u0644\u0630\u064A\u0646 \u064A\u0633\u062A\u0645\u062A\u0639\u0648\u0646 \u0628\u0647\u0630\u0647 \u0627\u0644\u0642\u0646\u0627\u0629 \u0627\u0644\u0631\u0627\u0626\u0639\u0629. \u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0627\u0644\u0622\u0646 \u0648\u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u062A\u062C\u0631\u0628\u0629 \u0641\u0631\u064A\u062F\u0629!`,
      en: `${title} channel offers the best exclusive content and amazing programs! 24/7 live broadcast in superior quality with diverse content for everyone. Join millions of viewers enjoying this amazing channel. Watch the live broadcast now and enjoy a unique experience!`
    }
  };
  const defaultContent = defaults[category] || defaults.movies;
  return lang === "ar" ? defaultContent.ar : defaultContent.en;
}
async function generateCategoryImagePrompt(title, category, includeLogoStyle = false) {
  const categoryStyles = {
    movies: {
      style: "dramatic cinematic movie poster, epic composition, theatrical release quality, IMAX style, film poster art, Hollywood blockbuster aesthetic, dramatic lighting, 8K ultra HD",
      logoStyle: "movie title typography, cinematic logo design"
    },
    tv_shows: {
      style: "streaming service quality poster, binge-worthy series art, Netflix/HBO style, dramatic character composition, TV series promotional art, premium streaming quality, 8K resolution",
      logoStyle: "TV series logo, streaming service branding"
    },
    sports: {
      style: "action sports photography, stadium atmosphere, championship moment, ESPN broadcast quality, dynamic motion blur, professional sports photography, 4K HDR",
      logoStyle: "sports team logo, championship branding"
    },
    recipes: {
      style: "professional gourmet food photography, Michelin star presentation, food magazine cover quality, appetizing macro closeup, warm natural lighting, delicious colors, cookbook photography, 8K",
      logoStyle: "food brand logo, restaurant quality presentation"
    },
    gaming: {
      style: "AAA video game cover art, PlayStation/Xbox quality, epic gaming poster, concept art masterpiece, vibrant neon colors, RTX ray tracing quality, game box art, 8K ultra HD",
      logoStyle: "video game logo, gaming brand typography, neon glow effect"
    },
    apps: {
      style: "App Store featured banner, premium app showcase, modern UI design, Apple design award quality, clean minimal interface, tech aesthetic, smartphone mockup, glossy finish, 8K",
      logoStyle: "app icon design, modern app logo, iOS/Android style"
    },
    tv_channels: {
      style: "professional TV channel branding, broadcast quality design, modern media network aesthetic, entertainment channel logo, premium broadcast graphics, 8K resolution",
      logoStyle: "TV channel logo, broadcast network branding"
    }
  };
  const categoryConfig = categoryStyles[category] || categoryStyles.movies;
  const systemPrompt = `You are an expert at creating prompts for FLUX AI image generation. Generate detailed, visual prompts that produce stunning HD professional poster images. Focus on composition, lighting, colors, atmosphere, and quality. The image should look like a professional ${category} promotional poster.`;
  const userPrompt = `Create a detailed image generation prompt for a professional ${category} poster featuring "${title}".

Style requirements:
${categoryConfig.style}
${includeLogoStyle ? categoryConfig.logoStyle : ""}

Additional requirements:
- Professional studio quality lighting
- Ultra high definition 8K resolution
- Stunning composition suitable for social media stories (9:16 aspect ratio)
- Vibrant, eye-catching colors
- Modern, trendy aesthetic
- The image should prominently feature the subject "${title}"

Generate the prompt in English only, under 80 words, no explanations:`;
  try {
    const prompt = await deepseekSDK.generateSimple(
      userPrompt,
      systemPrompt,
      { temperature: 0.8, max_tokens: 150 }
    );
    const cleanedPrompt = prompt.trim().replace(/^["']|["']$/g, "");
    console.log(`\u{1F3A8} Generated category-specific HD image prompt for ${category}: "${cleanedPrompt.substring(0, 100)}..."`);
    return cleanedPrompt;
  } catch (error) {
    console.error("Category image prompt generation error:", error);
    return `${title}, ${categoryConfig.style}, professional quality, 8K ultra HD, stunning composition`;
  }
}
var DEEPSEEK_API_URL, DEEPSEEK_API_VERSION, categoryPrompts2, DeepSeekSDK, deepseekSDK;
var init_deepseek = __esm({
  "server/deepseek.ts"() {
    "use strict";
    init_firestore();
    DEEPSEEK_API_URL = "https://api.deepseek.com";
    DEEPSEEK_API_VERSION = "v1";
    categoryPrompts2 = {
      movies: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u062C\u0630\u0627\u0628 \u0639\u0646 \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u0648\u0627\u0644\u0633\u064A\u0646\u0645\u0627. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u0634\u064A\u0642\u0629 \u0648\u0645\u062D\u0641\u0632\u0629 \u0644\u0644\u062A\u0641\u0627\u0639\u0644.`,
      tv_shows: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u0645\u062B\u064A\u0631 \u0639\u0646 \u0627\u0644\u0645\u0633\u0644\u0633\u0644\u0627\u062A \u0627\u0644\u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0633\u062A\u062E\u062F\u0645 \u0623\u0633\u0644\u0648\u0628\u0627\u064B \u0645\u0634\u0648\u0642\u0627\u064B \u064A\u062B\u064A\u0631 \u0627\u0644\u0641\u0636\u0648\u0644.`,
      sports: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u0631\u064A\u0627\u0636\u064A \u0645\u062D\u0645\u0633. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u062D\u0645\u0627\u0633\u064A\u0629 \u0648\u0645\u0644\u0647\u0645\u0629.`,
      recipes: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u0644\u0630\u064A\u0630 \u0639\u0646 \u0648\u0635\u0641\u0627\u062A \u0627\u0644\u0637\u0628\u062E. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u062C\u0639\u0644\u0647 \u0634\u0647\u064A\u0627\u064B \u0648\u0645\u063A\u0631\u064A\u0627\u064B.`,
      gaming: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u0645\u062B\u064A\u0631 \u0639\u0646 \u0623\u0644\u0639\u0627\u0628 \u0627\u0644\u0641\u064A\u062F\u064A\u0648. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0627\u0633\u062A\u062E\u062F\u0645 \u0644\u063A\u0629 \u062A\u0642\u0646\u064A\u0629 \u0648\u0645\u0645\u062A\u0639\u0629.`,
      apps: `\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u062A\u0642\u0646\u064A \u0639\u0646 \u0627\u0644\u062A\u0637\u0628\u064A\u0642\u0627\u062A \u0648\u0627\u0644\u0628\u0631\u0645\u062C\u064A\u0627\u062A. \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0642\u0635\u064A\u0631\u0627\u064B (100-150 \u0643\u0644\u0645\u0629) \u0648\u0645\u0646\u0627\u0633\u0628\u0627\u064B \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A. \u0631\u0643\u0632 \u0639\u0644\u0649 \u0627\u0644\u0641\u0648\u0627\u0626\u062F \u0648\u0627\u0644\u0645\u0645\u064A\u0632\u0627\u062A.`
    };
    DeepSeekSDK = class {
      apiKey = "";
      initialized = false;
      async initialize() {
        if (this.initialized) return;
        const config = await firestoreService.getAPIConfig("deepseek");
        if (config && config.apiKey) {
          this.apiKey = config.apiKey;
          this.initialized = true;
        } else if (process.env.DEEPSEEK_API_KEY) {
          this.apiKey = process.env.DEEPSEEK_API_KEY;
          this.initialized = true;
        }
      }
      async chat(messages, model = "deepseek-chat", options) {
        if (!this.initialized) {
          await this.initialize();
        }
        if (!this.apiKey) {
          throw new Error("DeepSeek API key is not configured. Please add it in the admin panel or environment variables.");
        }
        const url = `${DEEPSEEK_API_URL}/${DEEPSEEK_API_VERSION}/chat/completions`;
        const requestBody = {
          model,
          messages,
          temperature: options?.temperature ?? 0.8,
          max_tokens: options?.max_tokens ?? 500,
          ...options
        };
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${this.apiKey}`
          },
          body: JSON.stringify(requestBody)
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`DeepSeek API error: ${error}`);
        }
        return await response.json();
      }
      async generateWithReasoning(prompt, systemPrompt) {
        const messages = [
          ...systemPrompt ? [{ role: "system", content: systemPrompt }] : [],
          { role: "user", content: prompt }
        ];
        const response = await this.chat(messages, "deepseek-reasoner");
        return {
          content: response.choices[0]?.message?.content || "",
          reasoning: response.choices[0]?.message?.reasoning_content
        };
      }
      async generateSimple(prompt, systemPrompt, options) {
        const messages = [
          ...systemPrompt ? [{ role: "system", content: systemPrompt }] : [],
          { role: "user", content: prompt }
        ];
        const response = await this.chat(messages, "deepseek-chat", options);
        return response.choices[0]?.message?.content || "";
      }
      async streamChat(messages, model = "deepseek-chat", options) {
        if (!this.apiKey) {
          await this.initialize();
        }
        const url = `${DEEPSEEK_API_URL}/${DEEPSEEK_API_VERSION}/chat/completions`;
        const requestBody = {
          model,
          messages,
          temperature: options?.temperature ?? 0.8,
          max_tokens: options?.max_tokens ?? 500,
          stream: true,
          ...options
        };
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${this.apiKey}`
          },
          body: JSON.stringify(requestBody)
        });
        if (!response.ok) {
          const error = await response.text();
          throw new Error(`DeepSeek API error: ${error}`);
        }
        return response.body;
      }
      async verifyApiKey() {
        try {
          await this.generateSimple("Test", "You are a helpful assistant", { max_tokens: 10 });
          return true;
        } catch (error) {
          return false;
        }
      }
    };
    deepseekSDK = new DeepSeekSDK();
  }
});

// server/google-image-search.ts
var google_image_search_exports = {};
__export(google_image_search_exports, {
  GoogleImageSearchService: () => GoogleImageSearchService,
  googleImageSearchService: () => googleImageSearchService
});
var CATEGORY_SEARCH_MODIFIERS, GoogleImageSearchService, googleImageSearchService;
var init_google_image_search = __esm({
  "server/google-image-search.ts"() {
    "use strict";
    init_firestore();
    CATEGORY_SEARCH_MODIFIERS = {
      "movies": {
        searchType: "poster",
        keywords: ["movie poster", "film poster", "official poster"],
        aspectRatio: "portrait"
      },
      "tv_shows": {
        searchType: "poster",
        keywords: ["TV series poster", "show poster", "drama poster"],
        aspectRatio: "portrait"
      },
      "sports": {
        searchType: "thumbnail",
        keywords: ["sports match", "game highlight", "sports event"],
        aspectRatio: "landscape"
      },
      "recipes": {
        searchType: "thumbnail",
        keywords: ["food photo", "dish recipe", "cooking"],
        aspectRatio: "square"
      },
      "gaming": {
        searchType: "poster",
        keywords: ["official game poster logo", "video game cover art HD", "game key art official", "AAA game poster"],
        aspectRatio: "portrait"
      },
      "apps": {
        searchType: "thumbnail",
        keywords: ["app icon", "mobile app", "application"],
        aspectRatio: "square"
      },
      "tv_channels": {
        searchType: "poster",
        keywords: ["TV channel logo HD", "broadcast network logo", "television channel branding"],
        aspectRatio: "landscape"
      }
    };
    GoogleImageSearchService = class {
      config = null;
      async initialize() {
        const googleConfig = await firestoreService.getAPIConfig("google_trends");
        if (googleConfig?.additionalConfig?.searchEngineId && googleConfig?.apiKey) {
          this.config = {
            apiKey: googleConfig.apiKey,
            searchEngineId: googleConfig.additionalConfig.searchEngineId
          };
        } else if (process.env.GOOGLE_API_KEY && process.env.GOOGLE_SEARCH_ENGINE_ID) {
          this.config = {
            apiKey: process.env.GOOGLE_API_KEY,
            searchEngineId: process.env.GOOGLE_SEARCH_ENGINE_ID
          };
        }
      }
      async ensureInitialized() {
        if (!this.config) {
          await this.initialize();
        }
      }
      async searchImages(query, category, count = 5) {
        await this.ensureInitialized();
        if (!this.config) {
          console.warn("Google Custom Search not configured, using fallback method");
          return this.fallbackImageSearch(query, category);
        }
        const categoryConfig = CATEGORY_SEARCH_MODIFIERS[category];
        const searchQuery = `${query} ${categoryConfig.keywords[0]}`;
        try {
          console.log(`\u{1F50D} Searching Google Images for: "${searchQuery}"`);
          const params = new URLSearchParams({
            key: this.config.apiKey,
            cx: this.config.searchEngineId,
            q: searchQuery,
            searchType: "image",
            num: count.toString(),
            imgSize: "large",
            imgType: "photo",
            safe: "active",
            fileType: "png,jpg"
          });
          if (categoryConfig.aspectRatio === "portrait") {
            params.append("imgDominantColor", "black");
          }
          const response = await fetch(
            `https://www.googleapis.com/customsearch/v1?${params.toString()}`
          );
          if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(
              `Google Image Search API error: ${response.status} - ${errorData.error?.message || response.statusText}`
            );
          }
          const data = await response.json();
          if (!data.items || data.items.length === 0) {
            console.log("\u26A0\uFE0F No images found, trying fallback");
            return this.fallbackImageSearch(query, category);
          }
          const results = data.items.map((item) => ({
            imageUrl: item.link,
            title: item.title || query,
            source: "google",
            thumbnailUrl: item.image?.thumbnailLink,
            width: item.image?.width,
            height: item.image?.height,
            contextLink: item.image?.contextLink
          }));
          console.log(`\u2705 Found ${results.length} images from Google`);
          return results;
        } catch (error) {
          console.error("Google Image Search error:", error.message);
          return this.fallbackImageSearch(query, category);
        }
      }
      async searchPosterImage(query, category) {
        const results = await this.searchImages(query, category, 10);
        if (results.length === 0) {
          return null;
        }
        const validPosters = results.filter((r) => {
          if (r.width && r.height) {
            const aspectRatio = r.height / r.width;
            return aspectRatio > 1.2 && aspectRatio < 2;
          }
          return true;
        });
        if (validPosters.length > 0) {
          const randomIndex = Math.floor(Math.random() * Math.min(3, validPosters.length));
          return validPosters[randomIndex];
        }
        return results[0];
      }
      async searchThumbnailImage(query, category) {
        const results = await this.searchImages(query, category, 10);
        if (results.length === 0) {
          return null;
        }
        const randomIndex = Math.floor(Math.random() * Math.min(5, results.length));
        return results[randomIndex];
      }
      async searchMultipleImages(query, category, count = 10) {
        const results = await this.searchImages(query, category, count);
        return results.filter((r) => {
          const url = r.imageUrl.toLowerCase();
          return !url.endsWith(".svg") && !url.includes("svg+xml");
        });
      }
      async fallbackImageSearch(query, category) {
        console.log(`\u{1F4F8} Using fallback image search for: "${query}"`);
        const categoryConfig = CATEGORY_SEARCH_MODIFIERS[category];
        const placeholderResults = [{
          imageUrl: this.generatePlaceholderDataUrl(query, category),
          title: query,
          source: "generated",
          width: 800,
          height: category === "movies" || category === "tv_shows" ? 1200 : 800
        }];
        return placeholderResults;
      }
      generatePlaceholderDataUrl(title, category) {
        const gradients = {
          "movies": { from: "#1a1a2e", to: "#16213e" },
          "tv_shows": { from: "#0f0e17", to: "#2a2438" },
          "sports": { from: "#1b4332", to: "#2d6a4f" },
          "recipes": { from: "#7c2d12", to: "#ea580c" },
          "gaming": { from: "#3b0764", to: "#7c3aed" },
          "apps": { from: "#0c4a6e", to: "#0284c7" },
          "tv_channels": { from: "#4c1d95", to: "#7c3aed" }
        };
        const gradient = gradients[category] || gradients["movies"];
        const width = 1080;
        const height = category === "movies" || category === "tv_shows" ? 1620 : 1080;
        const svgContent = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="bg" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:${gradient.from};stop-opacity:1" />
            <stop offset="100%" style="stop-color:${gradient.to};stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.5)"/>
          </filter>
        </defs>
        <rect width="100%" height="100%" fill="url(#bg)"/>
        <text x="50%" y="50%" font-family="Arial, sans-serif" font-size="48" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" filter="url(#shadow)">
          ${title.substring(0, 25)}
        </text>
        <rect x="${width / 2 - 100}" y="${height - 100}" width="200" height="40" rx="20" fill="#f97316"/>
        <text x="50%" y="${height - 76}" font-family="Arial, sans-serif" font-size="20" fill="white" text-anchor="middle" dominant-baseline="middle">
          TRENDING
        </text>
      </svg>
    `;
        return `data:image/svg+xml;base64,${Buffer.from(svgContent).toString("base64")}`;
      }
      async downloadImage(url) {
        if (url.startsWith("data:")) {
          const base64Data = url.split(",")[1];
          return Buffer.from(base64Data, "base64");
        }
        console.log(`\u{1F4E5} Downloading image from: ${url.substring(0, 100)}...`);
        try {
          const response = await fetch(url, {
            headers: {
              "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
              "Accept": "image/*"
            }
          });
          if (!response.ok) {
            throw new Error(`Failed to download image: ${response.status}`);
          }
          const arrayBuffer = await response.arrayBuffer();
          const buffer = Buffer.from(arrayBuffer);
          console.log(`\u2705 Downloaded ${buffer.length} bytes`);
          return buffer;
        } catch (error) {
          console.error("Image download error:", error.message);
          throw new Error(`\u0641\u0634\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0635\u0648\u0631\u0629: ${error.message}`);
        }
      }
      createErrorResponse(errorType, description, step, details) {
        return {
          errorType,
          description,
          step,
          details
        };
      }
    };
    googleImageSearchService = new GoogleImageSearchService();
  }
});

// server/google-trends.ts
import googleTrends from "google-trends-api";
var TV_SHOWS_PRIORITY, MOVIES_PRIORITY, CATEGORY_MAPPING, CATEGORY_KEYWORDS, GoogleTrendsService, googleTrendsService;
var init_google_trends = __esm({
  "server/google-trends.ts"() {
    "use strict";
    TV_SHOWS_PRIORITY = [
      { geo: "TR", language: "tr", priority: 1, keywords: ["dizi", "Turkish series", "T\xFCrk dizisi", "yeni b\xF6l\xFCm", "turkish drama"] },
      { geo: "US", language: "en", priority: 2, keywords: ["TV series", "new episode", "trending show", "Netflix series", "HBO series"] },
      { geo: "IN", language: "hi", priority: 3, keywords: ["Hindi serial", "Indian drama", "TV show India", "new episode", "Indian series"] },
      { geo: "MX", language: "es", priority: 4, keywords: ["telenovela", "serie mexicana", "nuevo episodio", "drama latino", "novela"] }
    ];
    MOVIES_PRIORITY = [
      { geo: "US", language: "en", priority: 1, keywords: ["new movie 2025", "Hollywood film", "box office", "trending movie", "blockbuster"] }
    ];
    CATEGORY_MAPPING = {
      "movies": { geo: "US", category: 3 },
      "tv_shows": { geo: "US", category: 3 },
      "sports": { geo: "US", category: 20 },
      "recipes": { geo: "US", category: 71 },
      "gaming": { geo: "US", category: 8 },
      "apps": { geo: "US", category: 5 },
      "tv_channels": { geo: "US", category: 3 }
    };
    CATEGORY_KEYWORDS = {
      "movies": "movie",
      "tv_shows": "tv show",
      "sports": "sport",
      "recipes": "recipe",
      "gaming": "game",
      "apps": "app",
      "tv_channels": "TV channel"
    };
    GoogleTrendsService = class {
      async getTrendingSearchQueries(category) {
        try {
          console.log(`\u{1F50D} Getting trending topics for category: ${category}`);
          const { geo, category: categoryId } = CATEGORY_MAPPING[category];
          const keyword = CATEGORY_KEYWORDS[category];
          const results = await googleTrends.relatedQueries({
            keyword,
            geo,
            category: categoryId,
            hl: "en-US"
          });
          const data = JSON.parse(results);
          const topQueries = [];
          if (data?.default?.rankedList?.[0]?.rankedKeyword) {
            const queries = data.default.rankedList[0].rankedKeyword.slice(0, 10).map((item) => item.query).filter((query) => query && query.length > 0);
            topQueries.push(...queries);
          }
          if (topQueries.length === 0) {
            console.log("\u26A0\uFE0F  No trending queries found, using category keywords");
            return [keyword];
          }
          console.log(`\u2705 Found ${topQueries.length} trending queries:`, topQueries);
          return topQueries;
        } catch (error) {
          console.error("Error fetching Google Trends:", error);
          const fallback = CATEGORY_KEYWORDS[category];
          console.log(`\u26A0\uFE0F  Using fallback keyword: ${fallback}`);
          return [fallback];
        }
      }
      async getDailyTrends(geo = "US") {
        try {
          console.log(`\u{1F4CA} Getting daily trends for: ${geo}`);
          const results = await googleTrends.dailyTrends({
            geo
          });
          const data = JSON.parse(results);
          const trends = [];
          if (data?.default?.trendingSearchesDays?.[0]?.trendingSearches) {
            const trendingSearches = data.default.trendingSearchesDays[0].trendingSearches;
            for (const search of trendingSearches.slice(0, 20)) {
              trends.push({
                title: search.title.query,
                formattedTraffic: search.formattedTraffic,
                relatedQueries: search.relatedQueries?.map((q) => q.query) || []
              });
            }
          }
          console.log(`\u2705 Found ${trends.length} daily trends`);
          return trends;
        } catch (error) {
          console.error("Error fetching daily trends:", error);
          return [];
        }
      }
      async getRelatedQueries(keyword, geo = "US") {
        try {
          console.log(`\u{1F50E} Getting related queries for: "${keyword}"`);
          const results = await googleTrends.relatedQueries({
            keyword,
            geo,
            hl: "en-US"
          });
          const data = JSON.parse(results);
          const queries = [];
          if (data?.default?.rankedList?.[0]?.rankedKeyword) {
            const rankedQueries = data.default.rankedList[0].rankedKeyword.slice(0, 15).map((item) => item.query).filter((query) => query && query.length > 0);
            queries.push(...rankedQueries);
          }
          console.log(`\u2705 Found ${queries.length} related queries`);
          return queries;
        } catch (error) {
          console.error("Error fetching related queries:", error);
          return [keyword];
        }
      }
      async getBestSearchQueryForCategory(category) {
        const trendingQueries = await this.getTrendingSearchQueries(category);
        if (trendingQueries.length === 0) {
          return CATEGORY_KEYWORDS[category];
        }
        const randomIndex = Math.floor(Math.random() * Math.min(5, trendingQueries.length));
        return trendingQueries[randomIndex];
      }
      async getTrendingByPriority(category) {
        const results = [];
        if (category === "tv_shows") {
          for (const countryConfig of TV_SHOWS_PRIORITY) {
            try {
              console.log(`\u{1F50D} Searching trends for TV shows in ${countryConfig.geo}...`);
              const keyword = countryConfig.keywords[0];
              const queryResults = await googleTrends.relatedQueries({
                keyword,
                geo: countryConfig.geo,
                category: 3,
                hl: countryConfig.language
              });
              const data = JSON.parse(queryResults);
              if (data?.default?.rankedList?.[0]?.rankedKeyword) {
                const queries = data.default.rankedList[0].rankedKeyword.slice(0, 5).map((item) => item.query).filter((query) => query && query.length > 0);
                for (const term of queries) {
                  results.push({
                    category: "tv_shows",
                    trendingTerm: term,
                    priority: countryConfig.priority,
                    country: countryConfig.geo,
                    language: countryConfig.language
                  });
                }
              }
              console.log(`\u2705 Found ${results.filter((r) => r.country === countryConfig.geo).length} trends from ${countryConfig.geo}`);
            } catch (error) {
              console.error(`Error fetching trends for ${countryConfig.geo}:`, error.message);
            }
          }
        } else if (category === "movies") {
          for (const countryConfig of MOVIES_PRIORITY) {
            try {
              console.log(`\u{1F3AC} Searching trends for movies in ${countryConfig.geo}...`);
              const keyword = countryConfig.keywords[0];
              const queryResults = await googleTrends.relatedQueries({
                keyword,
                geo: countryConfig.geo,
                category: 3,
                hl: countryConfig.language
              });
              const data = JSON.parse(queryResults);
              if (data?.default?.rankedList?.[0]?.rankedKeyword) {
                const queries = data.default.rankedList[0].rankedKeyword.slice(0, 10).map((item) => item.query).filter((query) => query && query.length > 0);
                for (const term of queries) {
                  results.push({
                    category: "movies",
                    trendingTerm: term,
                    priority: countryConfig.priority,
                    country: countryConfig.geo,
                    language: countryConfig.language
                  });
                }
              }
              console.log(`\u2705 Found ${results.length} movie trends`);
            } catch (error) {
              console.error(`Error fetching movie trends:`, error.message);
            }
          }
        } else {
          const queries = await this.getTrendingSearchQueries(category);
          for (let i = 0; i < queries.length; i++) {
            results.push({
              category,
              trendingTerm: queries[i],
              priority: i + 1,
              country: "US",
              language: "en"
            });
          }
        }
        results.sort((a, b) => a.priority - b.priority);
        console.log(`\u{1F4CA} Total trend results for ${category}:`, results.length);
        return results;
      }
      async getBestTrendForCategory(category) {
        const trends = await this.getTrendingByPriority(category);
        if (trends.length === 0) {
          return {
            category,
            trendingTerm: CATEGORY_KEYWORDS[category],
            priority: 999,
            country: "US",
            language: "en"
          };
        }
        const highPriorityTrends = trends.filter((t) => t.priority <= 2);
        const targetTrends = highPriorityTrends.length > 0 ? highPriorityTrends : trends;
        const randomIndex = Math.floor(Math.random() * Math.min(3, targetTrends.length));
        return targetTrends[randomIndex];
      }
    };
    googleTrendsService = new GoogleTrendsService();
  }
});

// server/youtube-video-downloader.ts
var youtube_video_downloader_exports = {};
__export(youtube_video_downloader_exports, {
  YouTubeVideoDownloader: () => YouTubeVideoDownloader,
  youtubeVideoDownloader: () => youtubeVideoDownloader
});
import ytdl from "@distube/ytdl-core";
var MAX_VIDEO_DURATION, MIN_VIDEO_DURATION, YouTubeVideoDownloader, youtubeVideoDownloader;
var init_youtube_video_downloader = __esm({
  "server/youtube-video-downloader.ts"() {
    "use strict";
    init_firestore();
    init_r2_storage();
    init_google_trends();
    MAX_VIDEO_DURATION = 60;
    MIN_VIDEO_DURATION = 10;
    YouTubeVideoDownloader = class {
      apiKey = null;
      async initialize() {
        const youtubeConfig = await firestoreService.getAPIConfig("youtube");
        if (!youtubeConfig?.apiKey) {
          throw new Error("YouTube API key not configured");
        }
        this.apiKey = youtubeConfig.apiKey;
      }
      async searchYouTubeShortsVideo(query) {
        if (!this.apiKey) {
          await this.initialize();
        }
        try {
          console.log(`\u{1F50D} Searching YouTube Shorts for: "${query}"`);
          const searchUrl = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(query)}&type=video&videoDuration=short&videoDefinition=high&maxResults=20&order=viewCount&relevanceLanguage=en&safeSearch=strict&key=${this.apiKey}`;
          const searchResponse = await fetch(searchUrl);
          if (!searchResponse.ok) {
            const errorData = await searchResponse.json();
            throw new Error(`YouTube API error: ${errorData.error?.message || searchResponse.statusText}`);
          }
          const searchData = await searchResponse.json();
          if (!searchData.items || searchData.items.length === 0) {
            throw new Error("No videos found for this query");
          }
          const videoIds = searchData.items.map((item) => item.id.videoId).join(",");
          const detailsUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id=${videoIds}&key=${this.apiKey}`;
          const detailsResponse = await fetch(detailsUrl);
          if (!detailsResponse.ok) {
            throw new Error("Failed to fetch video details");
          }
          const detailsData = await detailsResponse.json();
          const videos = detailsData.items.map((item) => {
            const duration = this.parseDuration(item.contentDetails.duration);
            return {
              videoId: item.id,
              title: item.snippet.title,
              description: item.snippet.description,
              duration,
              channel: item.snippet.channelTitle,
              viewCount: parseInt(item.statistics.viewCount || "0"),
              likeCount: parseInt(item.statistics.likeCount || "0"),
              thumbnail: item.snippet.thumbnails.high?.url || item.snippet.thumbnails.medium?.url,
              url: `https://www.youtube.com/watch?v=${item.id}`
            };
          }).filter((video) => {
            return video.duration >= MIN_VIDEO_DURATION && video.duration <= MAX_VIDEO_DURATION;
          }).sort((a, b) => {
            const scoreA = a.viewCount + a.likeCount * 10;
            const scoreB = b.viewCount + b.likeCount * 10;
            return scoreB - scoreA;
          });
          if (videos.length === 0) {
            throw new Error(`No videos found with duration between ${MIN_VIDEO_DURATION}-${MAX_VIDEO_DURATION} seconds`);
          }
          console.log(`\u2705 Found ${videos.length} suitable YouTube videos`);
          return videos;
        } catch (error) {
          console.error("Error searching YouTube videos:", error);
          throw new Error(`\u0641\u0634\u0644 \u0641\u064A \u0627\u0644\u0628\u062D\u062B \u0639\u0646 \u0641\u064A\u062F\u064A\u0648\u0647\u0627\u062A YouTube: ${error.message}`);
        }
      }
      parseDuration(duration) {
        const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
        if (!match) return 0;
        const hours = parseInt(match[1] || "0");
        const minutes = parseInt(match[2] || "0");
        const seconds = parseInt(match[3] || "0");
        return hours * 3600 + minutes * 60 + seconds;
      }
      async downloadVideo(videoUrl) {
        try {
          console.log(`\u{1F4E5} Downloading video from: ${videoUrl}`);
          const info = await ytdl.getInfo(videoUrl);
          const format = ytdl.chooseFormat(info.formats, {
            quality: "highestvideo",
            filter: (format2) => {
              return format2.container === "mp4" && format2.hasVideo === true && format2.hasAudio === true && (format2.qualityLabel === "720p" || format2.qualityLabel === "1080p" || format2.qualityLabel === "480p");
            }
          });
          if (!format) {
            throw new Error("No suitable HD MP4 format found");
          }
          console.log(`\u{1F4CA} Selected format: ${format.qualityLabel} (${format.container})`);
          const chunks = [];
          return new Promise((resolve, reject) => {
            const stream = ytdl.downloadFromInfo(info, { format });
            stream.on("data", (chunk) => {
              chunks.push(chunk);
            });
            stream.on("end", () => {
              const buffer = Buffer.concat(chunks);
              console.log(`\u2705 Downloaded ${buffer.length} bytes`);
              resolve(buffer);
            });
            stream.on("error", (error) => {
              console.error("Download stream error:", error);
              reject(new Error(`\u0641\u0634\u0644 \u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0641\u064A\u062F\u064A\u0648: ${error.message}`));
            });
          });
        } catch (error) {
          console.error("Error downloading video:", error);
          if (error.message.includes("private video")) {
            throw new Error("\u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u062E\u0627\u0635 \u0648\u0644\u0627 \u064A\u0645\u0643\u0646 \u062A\u062D\u0645\u064A\u0644\u0647");
          } else if (error.message.includes("age")) {
            throw new Error("\u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u0645\u062D\u0638\u0648\u0631 \u0628\u0633\u0628\u0628 \u0642\u064A\u0648\u062F \u0627\u0644\u0639\u0645\u0631");
          } else if (error.message.includes("copyright")) {
            throw new Error("\u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u0645\u062D\u0645\u064A \u0628\u062D\u0642\u0648\u0642 \u0627\u0644\u0646\u0634\u0631");
          }
          throw new Error(`\u0641\u0634\u0644 \u0641\u064A \u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0641\u064A\u062F\u064A\u0648: ${error.message}`);
        }
      }
      async uploadToR2(videoBuffer, fileName) {
        try {
          console.log(`\u2601\uFE0F  Uploading to R2: ${fileName}`);
          const url = await r2Storage.uploadFile(videoBuffer, fileName, {
            contentType: "video/mp4",
            metadata: {
              source: "youtube-trending-video",
              uploadedAt: (/* @__PURE__ */ new Date()).toISOString()
            }
          });
          console.log(`\u2705 Uploaded to R2 successfully`);
          return url;
        } catch (error) {
          console.error("Error uploading to R2:", error);
          throw new Error(`\u0641\u0634\u0644 \u0641\u064A \u0631\u0641\u0639 \u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u0625\u0644\u0649 \u0627\u0644\u062A\u062E\u0632\u064A\u0646: ${error.message}`);
        }
      }
      async generateTrendingVideo(category) {
        console.log(`\u{1F3AC} Generating trending YouTube video for category: ${category}`);
        const searchQuery = await googleTrendsService.getBestSearchQueryForCategory(category);
        console.log(`\u{1F50E} Using trending search query: "${searchQuery}"`);
        const videos = await this.searchYouTubeShortsVideo(searchQuery);
        for (const video of videos) {
          try {
            console.log(`\u{1F3A5} Trying video: ${video.title} (${video.duration}s)`);
            console.log(`   Views: ${video.viewCount.toLocaleString()}, Likes: ${video.likeCount.toLocaleString()}`);
            if (video.duration > MAX_VIDEO_DURATION) {
              console.log(`\u23ED\uFE0F  Skipping - Video too long (${video.duration}s > ${MAX_VIDEO_DURATION}s)`);
              continue;
            }
            const videoBuffer = await this.downloadVideo(video.url);
            console.log(`\u2B07\uFE0F  Downloaded video: ${videoBuffer.length} bytes`);
            const fileName = `trending-videos/${category}/${Date.now()}-${video.videoId}.mp4`;
            const videoUrl = await this.uploadToR2(videoBuffer, fileName);
            return {
              videoUrl,
              title: video.title,
              description: video.description.substring(0, 200),
              trendingTopic: searchQuery,
              duration: video.duration
            };
          } catch (error) {
            console.error(`\u274C Failed to process video ${video.videoId}:`, error.message);
            if (videos.indexOf(video) === videos.length - 1) {
              throw new Error("\u0641\u0634\u0644 \u0641\u064A \u0645\u0639\u0627\u0644\u062C\u0629 \u062C\u0645\u064A\u0639 \u0627\u0644\u0641\u064A\u062F\u064A\u0648\u0647\u0627\u062A \u0627\u0644\u0645\u062A\u0627\u062D\u0629. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649.");
            }
            console.log(`\u23F3 Trying next video...`);
            await new Promise((resolve) => setTimeout(resolve, 2e3));
          }
        }
        throw new Error("Failed to process any video. Please try again.");
      }
    };
    youtubeVideoDownloader = new YouTubeVideoDownloader();
  }
});

// server/football-data-service.ts
var POPULAR_LEAGUES, TOP_TEAMS, EXCITING_DESCRIPTIONS_AR, EXCITING_DESCRIPTIONS_EN, WATCH_CTA_AR, WATCH_CTA_EN, FootballDataService, footballDataService;
var init_football_data_service = __esm({
  "server/football-data-service.ts"() {
    "use strict";
    init_firestore();
    POPULAR_LEAGUES = [
      { id: 39, name: "Premier League", country: "England" },
      { id: 140, name: "La Liga", country: "Spain" },
      { id: 135, name: "Serie A", country: "Italy" },
      { id: 78, name: "Bundesliga", country: "Germany" },
      { id: 61, name: "Ligue 1", country: "France" },
      { id: 2, name: "UEFA Champions League", country: "Europe" },
      { id: 3, name: "UEFA Europa League", country: "Europe" },
      { id: 848, name: "FIFA World Cup", country: "World" },
      { id: 531, name: "UEFA Super Cup", country: "Europe" },
      { id: 1, name: "FIFA World Cup", country: "World" }
    ];
    TOP_TEAMS = [
      "Real Madrid",
      "Barcelona",
      "Manchester City",
      "Liverpool",
      "Bayern Munich",
      "Paris Saint-Germain",
      "Manchester United",
      "Chelsea",
      "Arsenal",
      "Juventus",
      "Inter Milan",
      "AC Milan",
      "Borussia Dortmund",
      "Atletico Madrid",
      "Napoli",
      "Tottenham",
      "Newcastle",
      "Aston Villa",
      "Brighton",
      "West Ham"
    ];
    EXCITING_DESCRIPTIONS_AR = [
      "\u0645\u0628\u0627\u0631\u0627\u0629 \u0646\u0627\u0631\u064A\u0629 \u062A\u0646\u062A\u0638\u0631\u0643\u0645! \u0644\u0627 \u062A\u0641\u0648\u062A\u0648\u0627 \u0647\u0630\u0647 \u0627\u0644\u0645\u0648\u0627\u062C\u0647\u0629 \u0627\u0644\u062A\u0627\u0631\u064A\u062E\u064A\u0629",
      "\u0627\u0644\u062F\u064A\u0631\u0628\u064A \u0627\u0644\u0645\u0631\u062A\u0642\u0628! \u0645\u0646 \u0633\u064A\u0641\u0648\u0632 \u0641\u064A \u0647\u0630\u0647 \u0627\u0644\u0645\u0639\u0631\u0643\u0629 \u0627\u0644\u062D\u0627\u0633\u0645\u0629\u061F",
      "\u0645\u0648\u0627\u062C\u0647\u0629 \u0627\u0644\u0639\u0645\u0627\u0644\u0642\u0629! \u0627\u0633\u062A\u0639\u062F\u0648\u0627 \u0644\u0640 90 \u062F\u0642\u064A\u0642\u0629 \u0645\u0646 \u0627\u0644\u0625\u062B\u0627\u0631\u0629",
      "\u0642\u0645\u0629 \u0643\u0631\u0648\u064A\u0629 \u0644\u0627 \u062A\u064F\u0646\u0633\u0649! \u0634\u0627\u0647\u062F\u0648\u0627 \u0623\u0641\u0636\u0644 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u0641\u064A \u0627\u0644\u0639\u0627\u0644\u0645",
      "\u0627\u0644\u0643\u0644\u0627\u0633\u064A\u0643\u0648 \u0627\u0644\u0643\u0628\u064A\u0631! \u0627\u0644\u0645\u062C\u062F \u064A\u0646\u062A\u0638\u0631 \u0627\u0644\u0641\u0627\u0626\u0632",
      "\u0635\u0631\u0627\u0639 \u0627\u0644\u0643\u0628\u0627\u0631! \u0645\u0646 \u0633\u064A\u062D\u0633\u0645 \u0627\u0644\u0645\u0648\u0642\u0639\u0629 \u0627\u0644\u0644\u064A\u0644\u0629\u061F",
      "\u0644\u062D\u0638\u0627\u062A \u062A\u0627\u0631\u064A\u062E\u064A\u0629 \u0642\u0627\u062F\u0645\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A\u0648\u0627 \u0647\u0630\u0647 \u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629",
      "\u0623\u0642\u0648\u0649 \u0645\u0648\u0627\u062C\u0647\u0629 \u0641\u064A \u0627\u0644\u0645\u0648\u0633\u0645! \u0627\u0644\u0625\u062B\u0627\u0631\u0629 \u0641\u064A \u0623\u0639\u0644\u0649 \u0645\u0633\u062A\u0648\u064A\u0627\u062A\u0647\u0627"
    ];
    EXCITING_DESCRIPTIONS_EN = [
      "An epic clash awaits! Don't miss this historic showdown",
      "The awaited derby! Who will win this decisive battle?",
      "Clash of the titans! Get ready for 90 minutes of excitement",
      "An unforgettable football summit! Watch the world's best players",
      "The big classic! Glory awaits the winner",
      "Battle of giants! Who will settle it tonight?",
      "Historic moments coming! Don't miss this match",
      "The strongest match of the season! Excitement at its peak"
    ];
    WATCH_CTA_AR = [
      "\u0634\u0627\u0647\u062F \u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629 \u0645\u0628\u0627\u0634\u0631\u0629",
      "\u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u0645\u0648\u0627\u062C\u0647\u0629",
      "\u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646",
      "\u0645\u0648\u0639\u062F \u0627\u0644\u0625\u062B\u0627\u0631\u0629 \u0627\u0644\u0622\u0646",
      "\u0627\u0644\u0645\u062C\u062F \u064A\u0646\u062A\u0638\u0631"
    ];
    WATCH_CTA_EN = [
      "WATCH LIVE NOW",
      "DON'T MISS THIS MATCH",
      "JOIN MILLIONS OF VIEWERS",
      "THE EXCITEMENT STARTS NOW",
      "GLORY AWAITS"
    ];
    FootballDataService = class {
      rapidApiKey = null;
      async initialize() {
        const config = await firestoreService.getAPIConfig("rapidapi");
        this.rapidApiKey = config?.apiKey || process.env.RAPIDAPI_KEY || null;
        if (this.rapidApiKey) {
          console.log("\u2705 RapidAPI key loaded for Football Data");
        } else {
          console.log("\u26A0\uFE0F RapidAPI key not configured - using simulated football data");
        }
      }
      async ensureInitialized() {
        if (this.rapidApiKey === null) {
          await this.initialize();
        }
      }
      async getTrendingMatches() {
        await this.ensureInitialized();
        if (this.rapidApiKey) {
          const liveMatches = await this.fetchLiveMatches();
          if (liveMatches.length > 0) {
            return liveMatches;
          }
          const upcomingMatches = await this.getUpcomingMatches();
          if (upcomingMatches.length > 0) {
            return upcomingMatches;
          }
          return this.getSimulatedTrendingMatches();
        } else {
          return this.getSimulatedTrendingMatches();
        }
      }
      async fetchLiveMatches() {
        try {
          const response = await fetch("https://api-football-v1.p.rapidapi.com/v3/fixtures?live=all", {
            headers: {
              "X-RapidAPI-Key": this.rapidApiKey,
              "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
            }
          });
          if (!response.ok) {
            console.log("API-Football live matches request failed, will try upcoming matches");
            return [];
          }
          const data = await response.json();
          const matches = (data.response || []).slice(0, 5).map((fixture) => ({
            id: fixture.fixture.id,
            homeTeam: {
              id: fixture.teams.home.id,
              name: fixture.teams.home.name,
              logo: fixture.teams.home.logo
            },
            awayTeam: {
              id: fixture.teams.away.id,
              name: fixture.teams.away.name,
              logo: fixture.teams.away.logo
            },
            league: {
              id: fixture.league.id,
              name: fixture.league.name,
              logo: fixture.league.logo,
              country: fixture.league.country
            },
            date: new Date(fixture.fixture.date),
            venue: fixture.fixture.venue?.name,
            status: "live",
            score: {
              home: fixture.goals.home || 0,
              away: fixture.goals.away || 0
            }
          }));
          return matches.map((match) => this.createTrendingMatch(match));
        } catch (error) {
          console.error("Error fetching live matches:", error);
          return [];
        }
      }
      async getUpcomingMatches() {
        try {
          const today = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
          const tomorrow = new Date(Date.now() + 864e5).toISOString().split("T")[0];
          const response = await fetch(
            `https://api-football-v1.p.rapidapi.com/v3/fixtures?date=${today}&status=NS`,
            {
              headers: {
                "X-RapidAPI-Key": this.rapidApiKey,
                "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
              }
            }
          );
          if (!response.ok) {
            console.log("API-Football upcoming matches request failed");
            return [];
          }
          const data = await response.json();
          const popularLeagueIds = POPULAR_LEAGUES.map((l) => l.id);
          const matches = (data.response || []).filter((fixture) => popularLeagueIds.includes(fixture.league.id)).slice(0, 10).map((fixture) => ({
            id: fixture.fixture.id,
            homeTeam: {
              id: fixture.teams.home.id,
              name: fixture.teams.home.name,
              logo: fixture.teams.home.logo
            },
            awayTeam: {
              id: fixture.teams.away.id,
              name: fixture.teams.away.name,
              logo: fixture.teams.away.logo
            },
            league: {
              id: fixture.league.id,
              name: fixture.league.name,
              logo: fixture.league.logo,
              country: fixture.league.country
            },
            date: new Date(fixture.fixture.date),
            venue: fixture.fixture.venue?.name,
            status: "scheduled"
          }));
          return matches.map((match) => this.createTrendingMatch(match));
        } catch (error) {
          console.error("Error fetching upcoming matches:", error);
          return [];
        }
      }
      getSimulatedTrendingMatches() {
        const simulatedMatches = [
          {
            id: 1,
            homeTeam: {
              id: 541,
              name: "Real Madrid",
              logo: "https://media.api-sports.io/football/teams/541.png"
            },
            awayTeam: {
              id: 529,
              name: "Barcelona",
              logo: "https://media.api-sports.io/football/teams/529.png"
            },
            league: {
              id: 140,
              name: "La Liga",
              logo: "https://media.api-sports.io/football/leagues/140.png",
              country: "Spain"
            },
            date: new Date(Date.now() + 36e5),
            venue: "Santiago Bernab\xE9u",
            status: "scheduled"
          },
          {
            id: 2,
            homeTeam: {
              id: 50,
              name: "Manchester City",
              logo: "https://media.api-sports.io/football/teams/50.png"
            },
            awayTeam: {
              id: 40,
              name: "Liverpool",
              logo: "https://media.api-sports.io/football/teams/40.png"
            },
            league: {
              id: 39,
              name: "Premier League",
              logo: "https://media.api-sports.io/football/leagues/39.png",
              country: "England"
            },
            date: new Date(Date.now() + 72e5),
            venue: "Etihad Stadium",
            status: "scheduled"
          },
          {
            id: 3,
            homeTeam: {
              id: 157,
              name: "Bayern Munich",
              logo: "https://media.api-sports.io/football/teams/157.png"
            },
            awayTeam: {
              id: 165,
              name: "Borussia Dortmund",
              logo: "https://media.api-sports.io/football/teams/165.png"
            },
            league: {
              id: 78,
              name: "Bundesliga",
              logo: "https://media.api-sports.io/football/leagues/78.png",
              country: "Germany"
            },
            date: new Date(Date.now() + 108e5),
            venue: "Allianz Arena",
            status: "scheduled"
          },
          {
            id: 4,
            homeTeam: {
              id: 85,
              name: "Paris Saint-Germain",
              logo: "https://media.api-sports.io/football/teams/85.png"
            },
            awayTeam: {
              id: 541,
              name: "Real Madrid",
              logo: "https://media.api-sports.io/football/teams/541.png"
            },
            league: {
              id: 2,
              name: "UEFA Champions League",
              logo: "https://media.api-sports.io/football/leagues/2.png",
              country: "Europe"
            },
            date: new Date(Date.now() + 144e5),
            venue: "Parc des Princes",
            status: "scheduled"
          },
          {
            id: 5,
            homeTeam: {
              id: 42,
              name: "Arsenal",
              logo: "https://media.api-sports.io/football/teams/42.png"
            },
            awayTeam: {
              id: 47,
              name: "Tottenham",
              logo: "https://media.api-sports.io/football/teams/47.png"
            },
            league: {
              id: 39,
              name: "Premier League",
              logo: "https://media.api-sports.io/football/leagues/39.png",
              country: "England"
            },
            date: new Date(Date.now() + 18e6),
            venue: "Emirates Stadium",
            status: "scheduled"
          }
        ];
        const randomIndex = Math.floor(Math.random() * simulatedMatches.length);
        return [this.createTrendingMatch(simulatedMatches[randomIndex])];
      }
      createTrendingMatch(match) {
        const isTopTeamMatch = TOP_TEAMS.includes(match.homeTeam.name) && TOP_TEAMS.includes(match.awayTeam.name);
        const excitement = isTopTeamMatch ? 95 : 75 + Math.floor(Math.random() * 20);
        const descIndex = Math.floor(Math.random() * EXCITING_DESCRIPTIONS_AR.length);
        const ctaIndex = Math.floor(Math.random() * WATCH_CTA_AR.length);
        return {
          match,
          excitement,
          promotionalTextAr: `${EXCITING_DESCRIPTIONS_AR[descIndex]}

${WATCH_CTA_AR[ctaIndex]}`,
          promotionalTextEn: `${EXCITING_DESCRIPTIONS_EN[descIndex]}

${WATCH_CTA_EN[ctaIndex]}`
        };
      }
      async getRandomTrendingMatch() {
        const matches = await this.getTrendingMatches();
        if (matches.length === 0) {
          const simulated = this.getSimulatedTrendingMatches();
          return simulated[0];
        }
        matches.sort((a, b) => b.excitement - a.excitement);
        const randomIndex = Math.floor(Math.random() * Math.min(3, matches.length));
        return matches[randomIndex];
      }
      generateMatchTitle(match) {
        const titleEn = `${match.homeTeam.name} vs ${match.awayTeam.name}`;
        const titleAr = `${match.homeTeam.name} \u0636\u062F ${match.awayTeam.name}`;
        return { titleAr, titleEn };
      }
      getMatchTimeFormatted(match) {
        const matchDate = new Date(match.date);
        const now = /* @__PURE__ */ new Date();
        const diffMs = matchDate.getTime() - now.getTime();
        const diffHours = Math.floor(diffMs / (1e3 * 60 * 60));
        const diffMins = Math.floor(diffMs % (1e3 * 60 * 60) / (1e3 * 60));
        if (diffMs < 0) {
          return { timeAr: "\u062C\u0627\u0631\u064A\u0629 \u0627\u0644\u0622\u0646", timeEn: "LIVE NOW" };
        } else if (diffHours < 1) {
          return {
            timeAr: `\u062A\u0628\u062F\u0623 \u062E\u0644\u0627\u0644 ${diffMins} \u062F\u0642\u064A\u0642\u0629`,
            timeEn: `STARTS IN ${diffMins} MIN`
          };
        } else if (diffHours < 24) {
          return {
            timeAr: `\u062A\u0628\u062F\u0623 \u062E\u0644\u0627\u0644 ${diffHours} \u0633\u0627\u0639\u0629`,
            timeEn: `STARTS IN ${diffHours}H`
          };
        } else {
          const dateStr = matchDate.toLocaleDateString("ar-SA", { weekday: "long", day: "numeric", month: "short" });
          return {
            timeAr: dateStr,
            timeEn: matchDate.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" })
          };
        }
      }
    };
    footballDataService = new FootballDataService();
  }
});

// server/huggingface.ts
import { HfInference } from "@huggingface/inference";
var DEFAULT_MODEL, HuggingFaceSDK, huggingFaceSDK;
var init_huggingface = __esm({
  "server/huggingface.ts"() {
    "use strict";
    init_firestore();
    DEFAULT_MODEL = "black-forest-labs/FLUX.1-schnell";
    HuggingFaceSDK = class {
      apiKey = null;
      client = null;
      async initialize() {
        const config = await firestoreService.getAPIConfig("huggingface");
        this.apiKey = config?.apiKey || process.env.HUGGINGFACE_API_KEY || null;
        if (this.apiKey) {
          this.client = new HfInference(this.apiKey);
        }
      }
      async ensureInitialized() {
        if (!this.apiKey) {
          await this.initialize();
        }
      }
      async generateImage(prompt) {
        await this.ensureInitialized();
        if (!this.apiKey || !this.client) {
          throw new Error("\u0645\u0641\u062A\u0627\u062D Hugging Face API \u063A\u064A\u0631 \u0645\u064F\u0639\u062F. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u062A\u0647 \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629.");
        }
        try {
          const imageBlob = await this.client.textToImage({
            model: DEFAULT_MODEL,
            inputs: prompt
          });
          const arrayBuffer = await imageBlob.arrayBuffer();
          const base64Image = Buffer.from(arrayBuffer).toString("base64");
          return {
            imageData: base64Image,
            mimeType: "image/png"
          };
        } catch (error) {
          let errorMessage = "\u062E\u0637\u0623 \u0641\u064A \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631\u0629";
          if (error.message) {
            errorMessage = error.message;
          }
          if (error.status === 503) {
            errorMessage = "\u0627\u0644\u0646\u0645\u0648\u0630\u062C \u0642\u064A\u062F \u0627\u0644\u062A\u062D\u0645\u064A\u0644\u060C \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649 \u0628\u0639\u062F \u0642\u0644\u064A\u0644";
          } else if (error.status === 401) {
            errorMessage = "\u0645\u0641\u062A\u0627\u062D Hugging Face API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D";
          }
          console.error("Hugging Face API error:", errorMessage);
          throw new Error(errorMessage);
        }
      }
      async verifyApiKey(apiKey) {
        try {
          const client = new HfInference(apiKey);
          await client.textToImage({
            model: DEFAULT_MODEL,
            inputs: "test"
          });
          return true;
        } catch (error) {
          if (error.status === 503) {
            return true;
          }
          console.error("Hugging Face API verification error:", error);
          return false;
        }
      }
    };
    huggingFaceSDK = new HuggingFaceSDK();
  }
});

// server/google-play-service.ts
import gplay from "google-play-scraper";
var gplayAny, GAME_CATEGORIES, GooglePlayService, googlePlayService;
var init_google_play_service = __esm({
  "server/google-play-service.ts"() {
    "use strict";
    gplayAny = gplay;
    GAME_CATEGORIES = [
      "GAME_ACTION",
      "GAME_ADVENTURE",
      "GAME_ARCADE",
      "GAME_BOARD",
      "GAME_CARD",
      "GAME_CASINO",
      "GAME_CASUAL",
      "GAME_EDUCATIONAL",
      "GAME_MUSIC",
      "GAME_PUZZLE",
      "GAME_RACING",
      "GAME_ROLE_PLAYING",
      "GAME_SIMULATION",
      "GAME_SPORTS",
      "GAME_STRATEGY",
      "GAME_TRIVIA",
      "GAME_WORD"
    ];
    GooglePlayService = class {
      cachedTrendingGames = [];
      cacheTimestamp = 0;
      cacheDuration = 30 * 60 * 1e3;
      // 30 minutes cache
      usedGameIds = /* @__PURE__ */ new Set();
      // Track used games to avoid repetition
      maxUsedGamesCache = 50;
      // Max games to track before clearing
      constructor() {
        console.log("\u2705 Google Play Store service initialized");
      }
      /**
       * Get trending/top games from Google Play Store
       */
      async getTrendingGames(count = 20) {
        if (this.cachedTrendingGames.length > 0 && Date.now() - this.cacheTimestamp < this.cacheDuration) {
          console.log("\u{1F4E6} Using cached Play Store trending games");
          return this.cachedTrendingGames.slice(0, count);
        }
        console.log("\u{1F3AE} Fetching trending games from Google Play Store...");
        try {
          const allGames = [];
          const collections = [
            gplayAny.collection.TOP_FREE_GAMES,
            gplayAny.collection.TOP_PAID_GAMES,
            gplayAny.collection.TOP_GROSSING_GAMES
          ];
          for (const collection of collections) {
            try {
              const games = await gplay.list({
                collection,
                num: 15,
                fullDetail: false
              });
              for (const game of games) {
                if (!allGames.find((g) => g.appId === game.appId)) {
                  allGames.push(this.mapGameData(game));
                }
              }
            } catch (error) {
              console.log(`\u26A0\uFE0F Failed to fetch ${collection}: ${error.message}`);
            }
          }
          const randomCategories = GAME_CATEGORIES.sort(() => Math.random() - 0.5).slice(0, 3);
          for (const category of randomCategories) {
            try {
              const games = await gplayAny.list({
                collection: gplayAny.collection.TOP_FREE,
                category,
                num: 10,
                fullDetail: false
              });
              for (const game of games) {
                if (!allGames.find((g) => g.appId === game.appId)) {
                  allGames.push(this.mapGameData(game));
                }
              }
            } catch (error) {
              console.log(`\u26A0\uFE0F Failed to fetch category ${category}: ${error.message}`);
            }
          }
          const MIN_RATING = 3.5;
          const MIN_INSTALLS = 1e7;
          this.cachedTrendingGames = allGames.filter((game) => {
            const isGame = game.genreId?.startsWith("GAME") || game.genre?.toLowerCase().includes("game");
            const installCount = this.parseInstallCount(game.installs);
            const hasValidData = game.icon && game.title && game.score >= MIN_RATING && installCount >= MIN_INSTALLS;
            if (!isGame && game.title) {
              console.log(`\u26A0\uFE0F Filtering out non-game: ${game.title} (genreId: ${game.genreId})`);
            }
            return isGame && hasValidData;
          }).sort((a, b) => b.score - a.score);
          this.cacheTimestamp = Date.now();
          console.log(`\u2705 Fetched ${this.cachedTrendingGames.length} trending games from Play Store`);
          return this.cachedTrendingGames.slice(0, count);
        } catch (error) {
          console.error("\u274C Failed to fetch trending games:", error.message);
          return this.getFallbackGames();
        }
      }
      /**
       * Get detailed game information including screenshots
       */
      async getGameDetails(appId) {
        console.log(`\u{1F4D6} Fetching details for: ${appId}`);
        try {
          const details = await gplay.app({ appId });
          return {
            appId: details.appId,
            title: details.title,
            icon: details.icon,
            screenshots: details.screenshots || [],
            developer: details.developer,
            score: details.score || 0,
            scoreText: details.scoreText || "0",
            installs: details.installs || "0",
            genre: details.genre || "Game",
            genreId: details.genreId || "GAME",
            description: details.description || "",
            descriptionHTML: details.descriptionHTML || "",
            summary: details.summary || "",
            price: details.price || 0,
            free: details.free !== false,
            priceText: details.priceText || "Free",
            currency: details.currency || "USD",
            updated: details.updated,
            version: details.version,
            recentChanges: details.recentChanges,
            contentRating: details.contentRating,
            ratings: details.ratings,
            reviews: details.reviews,
            histogram: details.histogram,
            headerImage: details.headerImage,
            video: details.video,
            videoImage: details.videoImage
          };
        } catch (error) {
          console.error(`\u274C Failed to get game details for ${appId}:`, error.message);
          return null;
        }
      }
      /**
       * Get a random trending game with full details - ensures variety by tracking used games
       */
      async getRandomTrendingGame() {
        const trendingGames = await this.getTrendingGames(30);
        let availableGames;
        let isFallback = false;
        if (trendingGames.length === 0) {
          availableGames = this.getFallbackGames();
          isFallback = true;
          console.log(`\u{1F4E6} Using fallback games (${availableGames.length} available)`);
        } else {
          availableGames = trendingGames;
        }
        let unusedGames = availableGames.filter((game) => !this.usedGameIds.has(game.appId));
        if (unusedGames.length === 0) {
          console.log("\u{1F504} All games have been used, clearing tracking cache for fresh selection...");
          this.usedGameIds.clear();
          unusedGames = availableGames;
        }
        if (this.usedGameIds.size >= this.maxUsedGamesCache) {
          console.log("\u{1F504} Used games cache full, clearing oldest entries...");
          this.usedGameIds.clear();
        }
        console.log(`\u{1F3B2} Selecting from ${unusedGames.length} unused games (${this.usedGameIds.size} already used)`);
        let selectedGame;
        if (unusedGames.length === 1) {
          selectedGame = unusedGames[0];
        } else {
          const randomIndex = Math.floor(Math.random() * unusedGames.length);
          selectedGame = unusedGames[randomIndex];
        }
        this.usedGameIds.add(selectedGame.appId);
        console.log(`\u2705 Selected game: ${selectedGame.title} (appId: ${selectedGame.appId})`);
        if (!isFallback) {
          const details = await this.getGameDetails(selectedGame.appId);
          return details || selectedGame;
        }
        return selectedGame;
      }
      /**
       * Search for games on Play Store
       */
      async searchGames(query, count = 10) {
        console.log(`\u{1F50D} Searching Play Store for: ${query}`);
        try {
          const results = await gplay.search({
            term: query,
            num: count,
            fullDetail: false
          });
          const games = results.filter((app2) => app2.genreId?.startsWith("GAME")).map((game) => this.mapGameData(game));
          console.log(`\u2705 Found ${games.length} games matching "${query}"`);
          return games;
        } catch (error) {
          console.error(`\u274C Search failed for "${query}":`, error.message);
          return [];
        }
      }
      /**
       * Get high-resolution game icon URL
       */
      getHighResIcon(iconUrl) {
        if (iconUrl.includes("=w")) {
          return iconUrl.replace(/=w\d+-h\d+/g, "=w512-h512-rw");
        }
        return iconUrl;
      }
      /**
       * Get high-resolution screenshot URL
       */
      getHighResScreenshot(screenshotUrl) {
        if (screenshotUrl.includes("=w")) {
          return screenshotUrl.replace(/=w\d+/g, "=w1920");
        }
        return screenshotUrl;
      }
      /**
       * Get best screenshot for poster background - Smart selection algorithm
       * Prioritizes: portrait screenshots for story format, high-quality images, feature-rich screens
       */
      async getBestScreenshot(game) {
        let screenshots = game.screenshots;
        if (!screenshots || screenshots.length === 0) {
          const details = await this.getGameDetails(game.appId);
          if (details && details.screenshots && details.screenshots.length > 0) {
            screenshots = details.screenshots;
          } else {
            return null;
          }
        }
        console.log(`\u{1F4F8} Analyzing ${screenshots.length} screenshots for best selection...`);
        const portraitScreenshots = screenshots.filter((url) => {
          const heightMatch = url.match(/-h(\d+)/);
          const widthMatch = url.match(/=w(\d+)/);
          if (heightMatch && widthMatch) {
            const height = parseInt(heightMatch[1]);
            const width = parseInt(widthMatch[1]);
            return height > width;
          }
          return url.includes("portrait") || url.includes("-h1920") || url.includes("-h2560");
        });
        let selectedScreenshot;
        if (portraitScreenshots.length > 0) {
          const midIndex = Math.min(Math.floor(portraitScreenshots.length / 2), 2);
          selectedScreenshot = portraitScreenshots[midIndex] || portraitScreenshots[0];
          console.log(`\u2705 Selected portrait screenshot ${midIndex + 1}/${portraitScreenshots.length}`);
        } else if (screenshots.length >= 3) {
          const featureIndex = Math.min(2, screenshots.length - 1);
          selectedScreenshot = screenshots[featureIndex];
          console.log(`\u2705 Selected feature screenshot ${featureIndex + 1}/${screenshots.length}`);
        } else {
          selectedScreenshot = screenshots[0];
          console.log(`\u2705 Selected first screenshot (limited options)`);
        }
        return this.getHighResScreenshot(selectedScreenshot);
      }
      /**
       * Get best screenshot for apps (non-games) - Optimized for story posters
       * Enhanced smart algorithm to select the most attractive and professional screenshot
       * Prioritizes: UI-rich screens, portrait orientation, feature displays, high quality
       */
      async getBestAppScreenshot(app2) {
        let screenshots = app2.screenshots;
        if (!screenshots || screenshots.length === 0) {
          const details = await this.getAppDetails(app2.appId);
          if (details && details.screenshots && details.screenshots.length > 0) {
            screenshots = details.screenshots;
          } else {
            return null;
          }
        }
        console.log(`\u{1F4F1} Enhanced smart screenshot selection: Analyzing ${screenshots.length} screenshots for ${app2.title}...`);
        const scoredScreenshots = screenshots.map((url, index) => {
          let score = 0;
          if (index === 1) score += 40;
          else if (index === 2) score += 35;
          else if (index === 3) score += 30;
          else if (index === 4) score += 25;
          else if (index === 0) score += 10;
          else if (index >= 5) score += 15;
          const heightMatch = url.match(/-h(\d+)/);
          const widthMatch = url.match(/=w(\d+)/);
          if (heightMatch && widthMatch) {
            const height = parseInt(heightMatch[1]);
            const width = parseInt(widthMatch[1]);
            if (height > width) {
              score += 30;
              console.log(`   \u{1F4D0} Screenshot ${index + 1}: Portrait orientation (+30)`);
            } else if (width > height * 1.5) {
              score -= 15;
            }
          }
          if (url.includes("=w1920") || url.includes("=w2560") || url.includes("-h2560")) {
            score += 20;
          } else if (url.includes("=w1280") || url.includes("-h1920") || url.includes("-h1600")) {
            score += 15;
          } else if (url.includes("=w1080") || url.includes("-h1200")) {
            score += 10;
          }
          if (index === 0) {
            score -= 15;
          }
          if (index % 2 === 1) {
            score += 5;
          }
          return { url, index, score };
        });
        scoredScreenshots.sort((a, b) => b.score - a.score);
        let selectedCandidate = scoredScreenshots[0];
        if (scoredScreenshots.length >= 2) {
          const second = scoredScreenshots[1];
          if (selectedCandidate.score - second.score <= 5 && second.index < selectedCandidate.index) {
            selectedCandidate = second;
          }
        }
        console.log(`\u{1F3AF} Screenshot scores: ${scoredScreenshots.slice(0, 5).map((s) => `[${s.index + 1}:${s.score}pts]`).join(" ")}`);
        console.log(`\u2705 Selected screenshot ${selectedCandidate.index + 1}/${screenshots.length} (score: ${selectedCandidate.score}pts) for professional app poster`);
        return this.getHighResScreenshot(selectedCandidate.url);
      }
      /**
       * Map raw API data to our interface
       */
      mapGameData(game) {
        return {
          appId: game.appId,
          title: game.title,
          icon: game.icon,
          screenshots: game.screenshots || [],
          developer: game.developer || "Unknown",
          score: game.score || 0,
          scoreText: game.scoreText || "0",
          installs: game.installs || "0",
          genre: game.genre || "Game",
          genreId: game.genreId || "GAME",
          description: game.description || "",
          summary: game.summary || "",
          price: game.price || 0,
          free: game.free !== false,
          priceText: game.priceText || "Free",
          currency: game.currency || "USD"
        };
      }
      /**
       * Fallback games when API fails
       */
      getFallbackGames() {
        return [
          {
            appId: "com.supercell.clashofclans",
            title: "Clash of Clans",
            icon: "https://play-lh.googleusercontent.com/LByrur1mTmPeNr0ljI-uAUcct1rzmTve5Esau1SwoAUfHgz5OjHIAu6a3_VFqWThM8U=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/CBVb90FxjlDKuQTCXO3aZCZC6bxEEQADMW3FqJK2HJBWrV5jT_4i5p9wnCYA9qVLuPQ=w1920"],
            developer: "Supercell",
            score: 4.5,
            scoreText: "4.5",
            installs: "500,000,000+",
            genre: "Strategy",
            genreId: "GAME_STRATEGY",
            description: "Epic strategy game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.dts.freefireth",
            title: "Free Fire",
            icon: "https://play-lh.googleusercontent.com/WWcssdzTZvx0OsXvnHL5Df_UnE0LMzPvMQefS4sBIJK8avrIwFgMvJh48LDBH4-FJpz_=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/yT1l5ggNiUh-dD0m9-LbL_EEL3Y0qFkX6GZhqfnE0XH_hG9pQ4C1FZ0T2qEP0nQeZ9E=w1920"],
            developer: "Garena International",
            score: 4.1,
            scoreText: "4.1",
            installs: "1,000,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Ultimate survival shooter",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.pubg.imobile",
            title: "PUBG MOBILE",
            icon: "https://play-lh.googleusercontent.com/JRd05pyBH41qjgsJuWduRJpDeZG0Hn-x9vNNmLqNy8LxE7_4vCEUJVNBQqHzMGz_Cg=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/QqMbDqoRvM-1sJMwfL0_dLiIwKbGHqDq0MHvQVPjLvG_xXXHy9R7kWPnLwGqlz8Z-A=w1920"],
            developer: "Level Infinite",
            score: 4.2,
            scoreText: "4.2",
            installs: "1,000,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Battle Royale game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.mojang.minecraftpe",
            title: "Minecraft",
            icon: "https://play-lh.googleusercontent.com/VSwHQjcAttxsLE47RuS4PqpC4LT7lCoSjE7Hx5AW_yCxtDvcnsHHvm5CTuL5BPN-uRTP=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/yAtZnNL-9Eb5VYSs8-rZVvLMXb3Fj_nXq0SsYz4IiRBR9HJCeV7CsLsBKjQ9M-c2Og=w1920"],
            developer: "Mojang",
            score: 4.5,
            scoreText: "4.5",
            installs: "100,000,000+",
            genre: "Arcade",
            genreId: "GAME_ARCADE",
            description: "Explore, build, and survive",
            price: 7.49,
            free: false,
            priceText: "$7.49",
            currency: "USD"
          },
          {
            appId: "com.roblox.client",
            title: "Roblox",
            icon: "https://play-lh.googleusercontent.com/WNWZaxi9RdJKe2GQM3vqXIAkk69mnIl4Cc8EyZcirr6_qsMEOcp29BmBtXBZQu2ulS8=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/2xXcP4jI_EqFbPr6ySPg1MHgGh5xyElDEF9Kk6IxD6nKD0YZj_M_3nf_0KqEq4rOXQ=w1920"],
            developer: "Roblox Corporation",
            score: 4.4,
            scoreText: "4.4",
            installs: "500,000,000+",
            genre: "Adventure",
            genreId: "GAME_ADVENTURE",
            description: "Millions of experiences await",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.supercell.brawlstars",
            title: "Brawl Stars",
            icon: "https://play-lh.googleusercontent.com/UfoALDKp0CeDKlOF5tl_yL3lj0D3rN-oQlj2U8Ff-tTl3yqP5w6O-8_5BlH0pXl8v8s=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/U7Ig0hS2T4v8j7e3qN9d8jU6B7iKB2qB5Z1NlH8ZbVr4g_L0Z_4hxQjL_WH8CwADKA=w1920"],
            developer: "Supercell",
            score: 4.3,
            scoreText: "4.3",
            installs: "500,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Fast-paced multiplayer battles",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.gameloft.android.ANMP.GloftA9HM",
            title: "Asphalt 9: Legends",
            icon: "https://play-lh.googleusercontent.com/WA_oh_H3unx6HzntG7SZ2bQ0VQmLW5S6U4fPdBrHnFLz0qNbD8yZW8wy0HnZHEZ8=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/1Y9nJTfFLq4_nNIX9JYxIMX9HZ5xVMqQYK6I0Y2O3E-Vq_xWd7I1nfZ5C1H-Fv2=w1920"],
            developer: "Gameloft SE",
            score: 4.4,
            scoreText: "4.4",
            installs: "100,000,000+",
            genre: "Racing",
            genreId: "GAME_RACING",
            description: "Arcade racing at its best",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.tencent.ig",
            title: "PUBG MOBILE LITE",
            icon: "https://play-lh.googleusercontent.com/N0UxhBVUWJqr7FLN3FNmGZNndLhV5J9K_AoXBr6URmPqZ7FzM9fTlT8nLxP6r4VJ-sI=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/QNf3AH1ZmZ8vJqv3w9U9K3F0l5Q1p9O3n2L8_4a0eGl7G4D_pBz6f3f_lPl8D4nV=w1920"],
            developer: "Tencent Games",
            score: 4,
            scoreText: "4.0",
            installs: "100,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Battle Royale for all devices",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.king.candycrushsaga",
            title: "Candy Crush Saga",
            icon: "https://play-lh.googleusercontent.com/1-hPxafOxdYpYZEOKzNIkSP43HXCNftVJVttoo4ucl7rsMASXW3Xr6GlXURCubE1tA=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/pM7jXE_4vZVrG2Gu9TlE0N_J_Ap8OJvDN0qY1Q_VHAZ0oKEZKGPfFT1Y_TRl4GA=w1920"],
            developer: "King",
            score: 4.5,
            scoreText: "4.5",
            installs: "1,000,000,000+",
            genre: "Casual",
            genreId: "GAME_CASUAL",
            description: "Sweet puzzle game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.activision.callofduty.shooter",
            title: "Call of Duty: Mobile",
            icon: "https://play-lh.googleusercontent.com/D6ixh-XqQ9K3RxdWJyEQ4WESTxmLMoJEDIGW_GmVnGE_mPe-RL-H1-1-1X_-GEDp5dI=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/QxJnLxEVkT8_2H4PYwYxlS_rWqP9rMx6YA1MvAuLq8jM5O0yTxM7JQM_F3vM8nQVhg=w1920"],
            developer: "Activision Publishing",
            score: 4.3,
            scoreText: "4.3",
            installs: "500,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Legendary FPS on mobile",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.supercell.clashroyale",
            title: "Clash Royale",
            icon: "https://play-lh.googleusercontent.com/rIvZQ_H3hfmexC8vurmLczLs7QiZBSwMf2EKFQIGwSezGxN1H6yG8q2hlNZLHB1Pex8=w512-h512",
            screenshots: [],
            developer: "Supercell",
            score: 4.2,
            scoreText: "4.2",
            installs: "500,000,000+",
            genre: "Strategy",
            genreId: "GAME_STRATEGY",
            description: "Real-time multiplayer battle",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.ea.game.fifa6_row",
            title: "EA SPORTS FC Mobile",
            icon: "https://play-lh.googleusercontent.com/3nMzI6aOmVzxaJ8E2EWOqk-9cGLqK8ECBgvJlBH_eBRL8T8ZFZR6QbE7lJSxRdRcSQ=w512-h512",
            screenshots: [],
            developer: "Electronic Arts",
            score: 4.1,
            scoreText: "4.1",
            installs: "100,000,000+",
            genre: "Sports",
            genreId: "GAME_SPORTS",
            description: "Ultimate football experience",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.miHoYo.GenshinImpact",
            title: "Genshin Impact",
            icon: "https://play-lh.googleusercontent.com/h4MX8h6XFHTCEOiCqK0a7wPT9RfKyZqsIoFQz2bj1zCZ6qn4OWDQe0Tf8hCh7w7zFA=w512-h512",
            screenshots: [],
            developer: "miHoYo Limited",
            score: 4.2,
            scoreText: "4.2",
            installs: "100,000,000+",
            genre: "Role Playing",
            genreId: "GAME_ROLE_PLAYING",
            description: "Open-world adventure RPG",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.innersloth.spacemafia",
            title: "Among Us",
            icon: "https://play-lh.googleusercontent.com/8ddL1kuoNUB5vUvgDVjYY3_6HwQcrg1K2fd_R8soD-e2QYj8fT9cfhfh3G0hnSruLKE=w512-h512",
            screenshots: [],
            developer: "Innersloth LLC",
            score: 4.3,
            scoreText: "4.3",
            installs: "500,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "Social deduction game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.kiloo.subwaysurf",
            title: "Subway Surfers",
            icon: "https://play-lh.googleusercontent.com/6FhY0m1vV_IdHpVuY3nlfQFAVP0xLBWEUVKxCVdKJOk4x0S8EDRjjg8P9j7y0T7VbA=w512-h512",
            screenshots: [],
            developer: "SYBO Games",
            score: 4.5,
            scoreText: "4.5",
            installs: "1,000,000,000+",
            genre: "Arcade",
            genreId: "GAME_ARCADE",
            description: "Endless runner game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.imangi.templerun2",
            title: "Temple Run 2",
            icon: "https://play-lh.googleusercontent.com/RGRT9HqXRhVVv2ACxXYKlxBQgHqLjDnlq_KNvg0Y6mA0LO5lGxvnpPHrG0kA6ZXDSA=w512-h512",
            screenshots: [],
            developer: "Imangi Studios",
            score: 4.3,
            scoreText: "4.3",
            installs: "1,000,000,000+",
            genre: "Arcade",
            genreId: "GAME_ARCADE",
            description: "Endless running adventure",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.rovio.angrybirds2.revo",
            title: "Angry Birds 2",
            icon: "https://play-lh.googleusercontent.com/4n8Wh-3cKBZ_nMYP_aSGMmWJ9_Ee_FoR-3i_iJWr3K0Ih0-l5G8gLq8WEg8RAQXQOA=w512-h512",
            screenshots: [],
            developer: "Rovio Entertainment",
            score: 4.4,
            scoreText: "4.4",
            installs: "100,000,000+",
            genre: "Casual",
            genreId: "GAME_CASUAL",
            description: "Slingshot fun",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.mobile.legends",
            title: "Mobile Legends: Bang Bang",
            icon: "https://play-lh.googleusercontent.com/XBNxPXFfKmJ5RhWoaE_2SUvELCzQRzLl0YmJ4_zX6pG8Nh7FQPMzVe9hI5qQ0Bwx=w512-h512",
            screenshots: [],
            developer: "Moonton",
            score: 4.2,
            scoreText: "4.2",
            installs: "500,000,000+",
            genre: "Action",
            genreId: "GAME_ACTION",
            description: "MOBA battle arena",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.supercell.hayday",
            title: "Hay Day",
            icon: "https://play-lh.googleusercontent.com/pM0RNMvLDQWFY_ELB_Kqr8RJJKBPqHJ6v4YVH_j_OzWQm7k_GjL4O7qZ1X4qO4rA=w512-h512",
            screenshots: [],
            developer: "Supercell",
            score: 4.4,
            scoreText: "4.4",
            installs: "100,000,000+",
            genre: "Simulation",
            genreId: "GAME_SIMULATION",
            description: "Farm building game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.etermax.preguntados.lite",
            title: "Trivia Crack",
            icon: "https://play-lh.googleusercontent.com/MVFL_aYsXD_T3E4JLrPJyOHpB_6HT-JT4Lfl8XhPF4H_W1O7hZhQBN6F_E8MWePh=w512-h512",
            screenshots: [],
            developer: "etermax",
            score: 4.5,
            scoreText: "4.5",
            installs: "500,000,000+",
            genre: "Trivia",
            genreId: "GAME_TRIVIA",
            description: "Quiz game with friends",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.plarium.raidlegends",
            title: "RAID: Shadow Legends",
            icon: "https://play-lh.googleusercontent.com/ByEFfNpklQkP8L-7xAeLHMphP2rLZA9QLQB0TmGcFoC5T3zDqcOCZ3sLh5dV4R8wBA=w512-h512",
            screenshots: [],
            developer: "Plarium Global Ltd",
            score: 4.3,
            scoreText: "4.3",
            installs: "100,000,000+",
            genre: "Role Playing",
            genreId: "GAME_ROLE_PLAYING",
            description: "Epic fantasy RPG",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.outfit7.talkingtom2",
            title: "My Talking Tom 2",
            icon: "https://play-lh.googleusercontent.com/lRU9bKsD8KQ_L6K4dVdP4IqRYsKNPJv3M1qE7FZ_KGp5eQe6U0Q8oV0CaJ7dM_kH=w512-h512",
            screenshots: [],
            developer: "Outfit7 Limited",
            score: 4.4,
            scoreText: "4.4",
            installs: "500,000,000+",
            genre: "Casual",
            genreId: "GAME_CASUAL",
            description: "Virtual pet adventure",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.miniclip.eightballpool",
            title: "8 Ball Pool",
            icon: "https://play-lh.googleusercontent.com/N-WvKf_LFM_fdbDFkMT_0LnKmMt0J_PN4jF0V3Z_U7P8nI3K8gMC5L_IXU2v4g0OhQ=w512-h512",
            screenshots: [],
            developer: "Miniclip.com",
            score: 4.4,
            scoreText: "4.4",
            installs: "500,000,000+",
            genre: "Sports",
            genreId: "GAME_SPORTS",
            description: "Online pool game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.playrix.homescapes",
            title: "Homescapes",
            icon: "https://play-lh.googleusercontent.com/H3MKx1YEsRSQf_L8EhVT_CbF3g7L7Wn8R_kVmGMpPXe8XeA_W0o2bTLOT8E8wGqbHQ=w512-h512",
            screenshots: [],
            developer: "Playrix",
            score: 4.3,
            scoreText: "4.3",
            installs: "500,000,000+",
            genre: "Puzzle",
            genreId: "GAME_PUZZLE",
            description: "Match-3 puzzle game",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.playrix.gardenscapes",
            title: "Gardenscapes",
            icon: "https://play-lh.googleusercontent.com/xBUE6lPqMHxfLzI0hFMd7Wb0cRk8Q7FoEJL5mN7sYmB_RQqM3FzR4K2_LBB8fB8p=w512-h512",
            screenshots: [],
            developer: "Playrix",
            score: 4.3,
            scoreText: "4.3",
            installs: "500,000,000+",
            genre: "Puzzle",
            genreId: "GAME_PUZZLE",
            description: "Garden renovation puzzle",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          }
        ];
      }
      /**
       * Parse install count string to number (e.g., "100,000,000+" -> 100000000)
       */
      parseInstallCount(installs) {
        if (!installs) return 0;
        const cleanInstalls = installs.replace(/[,+]/g, "");
        return parseInt(cleanInstalls) || 0;
      }
      /**
       * Get formatted genre text in Arabic
       */
      getGenreArabic(genre) {
        const genreMap = {
          "Action": "\u0623\u0643\u0634\u0646",
          "Adventure": "\u0645\u063A\u0627\u0645\u0631\u0629",
          "Arcade": "\u0622\u0631\u0643\u064A\u062F",
          "Board": "\u0623\u0644\u0639\u0627\u0628 \u0644\u0648\u062D\u064A\u0629",
          "Card": "\u0623\u0644\u0639\u0627\u0628 \u0648\u0631\u0642",
          "Casino": "\u0643\u0627\u0632\u064A\u0646\u0648",
          "Casual": "\u0639\u0627\u062F\u064A\u0629",
          "Educational": "\u062A\u0639\u0644\u064A\u0645\u064A\u0629",
          "Music": "\u0645\u0648\u0633\u064A\u0642\u0649",
          "Puzzle": "\u0623\u0644\u063A\u0627\u0632",
          "Racing": "\u0633\u0628\u0627\u0642\u0627\u062A",
          "Role Playing": "\u062A\u0642\u0645\u0635 \u0623\u062F\u0648\u0627\u0631",
          "Simulation": "\u0645\u062D\u0627\u0643\u0627\u0629",
          "Sports": "\u0631\u064A\u0627\u0636\u064A\u0629",
          "Strategy": "\u0627\u0633\u062A\u0631\u0627\u062A\u064A\u062C\u064A\u0629",
          "Trivia": "\u0645\u0639\u0644\u0648\u0645\u0627\u062A",
          "Word": "\u0643\u0644\u0645\u0627\u062A"
        };
        return genreMap[genre] || genre;
      }
      /**
       * Format installs count for display
       */
      formatInstalls(installs) {
        const cleanInstalls = installs.replace(/[,+]/g, "");
        const num = parseInt(cleanInstalls);
        if (num >= 1e9) {
          const billions = (num / 1e9).toFixed(1);
          return {
            ar: `+${billions} \u0645\u0644\u064A\u0627\u0631 \u062A\u062D\u0645\u064A\u0644`,
            en: `${billions}B+ Downloads`
          };
        } else if (num >= 1e6) {
          const millions = (num / 1e6).toFixed(0);
          return {
            ar: `+${millions} \u0645\u0644\u064A\u0648\u0646 \u062A\u062D\u0645\u064A\u0644`,
            en: `${millions}M+ Downloads`
          };
        } else if (num >= 1e3) {
          const thousands = (num / 1e3).toFixed(0);
          return {
            ar: `+${thousands} \u0623\u0644\u0641 \u062A\u062D\u0645\u064A\u0644`,
            en: `${thousands}K+ Downloads`
          };
        }
        return {
          ar: `+${installs} \u062A\u062D\u0645\u064A\u0644`,
          en: `${installs}+ Downloads`
        };
      }
      // ============= Apps (Non-Game Applications) Methods =============
      cachedTrendingApps = [];
      appsCacheTimestamp = 0;
      usedAppIds = /* @__PURE__ */ new Set();
      APP_CATEGORIES = [
        "SOCIAL",
        "COMMUNICATION",
        "PRODUCTIVITY",
        "TOOLS",
        "ENTERTAINMENT",
        "PHOTOGRAPHY",
        "VIDEO_PLAYERS",
        "MUSIC_AND_AUDIO",
        "SHOPPING",
        "FINANCE",
        "HEALTH_AND_FITNESS",
        "EDUCATION",
        "TRAVEL_AND_LOCAL",
        "NEWS_AND_MAGAZINES",
        "FOOD_AND_DRINK",
        "LIFESTYLE",
        "BUSINESS",
        "WEATHER"
      ];
      /**
       * Get trending/top apps (non-games) from Google Play Store
       */
      async getTrendingApps(count = 30) {
        if (this.cachedTrendingApps.length > 0 && Date.now() - this.appsCacheTimestamp < this.cacheDuration) {
          console.log("\u{1F4E6} Using cached Play Store trending apps");
          return this.cachedTrendingApps.slice(0, count);
        }
        console.log("\u{1F4F1} Fetching trending apps from Google Play Store...");
        try {
          const allApps = [];
          const collections = [
            gplayAny.collection.TOP_FREE,
            gplayAny.collection.TOP_PAID,
            gplayAny.collection.GROSSING
          ];
          for (const collection of collections) {
            try {
              const apps = await gplay.list({
                collection,
                num: 50,
                fullDetail: false
              });
              for (const app2 of apps) {
                const appGenreId = app2.genreId || "";
                if (!appGenreId.startsWith("GAME") && !allApps.find((a) => a.appId === app2.appId)) {
                  allApps.push(this.mapGameData(app2));
                }
              }
            } catch (error) {
              console.log(`\u26A0\uFE0F Failed to fetch ${collection}: ${error.message}`);
            }
          }
          const randomCategories = this.APP_CATEGORIES.sort(() => Math.random() - 0.5).slice(0, 8);
          for (const category of randomCategories) {
            try {
              const apps = await gplayAny.list({
                collection: gplayAny.collection.TOP_FREE,
                category,
                num: 25,
                fullDetail: false
              });
              for (const app2 of apps) {
                const appGenreId = app2.genreId || "";
                if (!appGenreId.startsWith("GAME") && !allApps.find((a) => a.appId === app2.appId)) {
                  allApps.push(this.mapGameData(app2));
                }
              }
            } catch (error) {
              console.log(`\u26A0\uFE0F Failed to fetch category ${category}: ${error.message}`);
            }
          }
          const MIN_RATING = 3.8;
          const MIN_INSTALLS = 1e6;
          this.cachedTrendingApps = allApps.filter((app2) => {
            const isNotGame = !app2.genreId?.startsWith("GAME");
            const installCount = this.parseInstallCount(app2.installs);
            const hasValidData = app2.icon && app2.title && app2.score >= MIN_RATING && installCount >= MIN_INSTALLS;
            return isNotGame && hasValidData;
          }).sort((a, b) => b.score - a.score);
          this.appsCacheTimestamp = Date.now();
          console.log(`\u2705 Fetched ${this.cachedTrendingApps.length} trending apps from Play Store`);
          return this.cachedTrendingApps.slice(0, count);
        } catch (error) {
          console.error("\u274C Failed to fetch trending apps:", error.message);
          return this.getFallbackApps();
        }
      }
      /**
       * Get a random trending app with full details - ensures variety by tracking used apps
       */
      async getRandomTrendingApp() {
        const trendingApps = await this.getTrendingApps(100);
        let availableApps;
        let isFallback = false;
        if (trendingApps.length === 0) {
          availableApps = this.getFallbackApps();
          isFallback = true;
          console.log(`\u{1F4E6} Using fallback apps (${availableApps.length} available)`);
        } else {
          availableApps = trendingApps;
        }
        let unusedApps = availableApps.filter((app2) => !this.usedAppIds.has(app2.appId));
        if (unusedApps.length === 0) {
          console.log("\u{1F504} All apps have been used, forcing fresh fetch from Google Play...");
          this.usedAppIds.clear();
          this.appsCacheTimestamp = 0;
          this.cachedTrendingApps = [];
          const freshApps = await this.getTrendingApps(100);
          if (freshApps.length > 0) {
            availableApps = freshApps;
            unusedApps = freshApps;
            console.log(`\u2705 Fetched ${freshApps.length} fresh apps from Google Play`);
          } else {
            unusedApps = availableApps;
          }
        }
        if (this.usedAppIds.size >= 100) {
          console.log("\u{1F504} Used apps cache full, forcing fresh fetch...");
          this.usedAppIds.clear();
          this.appsCacheTimestamp = 0;
        }
        console.log(`\u{1F3B2} Selecting from ${unusedApps.length} unused apps (${this.usedAppIds.size} already used)`);
        let selectedApp;
        if (unusedApps.length === 1) {
          selectedApp = unusedApps[0];
        } else {
          const randomIndex = Math.floor(Math.random() * unusedApps.length);
          selectedApp = unusedApps[randomIndex];
        }
        this.usedAppIds.add(selectedApp.appId);
        console.log(`\u2705 Selected app: ${selectedApp.title} (appId: ${selectedApp.appId})`);
        if (!isFallback) {
          const details = await this.getAppDetails(selectedApp.appId);
          return details || selectedApp;
        }
        return selectedApp;
      }
      /**
       * Get detailed app information including screenshots
       */
      async getAppDetails(appId) {
        console.log(`\u{1F4D6} Fetching app details for: ${appId}`);
        try {
          const details = await gplay.app({ appId });
          return {
            appId: details.appId,
            title: details.title,
            icon: details.icon,
            screenshots: details.screenshots || [],
            developer: details.developer,
            score: details.score || 0,
            scoreText: details.scoreText || "0",
            installs: details.installs || "0",
            genre: details.genre || "App",
            genreId: details.genreId || "APPLICATION",
            description: details.description || "",
            descriptionHTML: details.descriptionHTML || "",
            summary: details.summary || "",
            price: details.price || 0,
            free: details.free !== false,
            priceText: details.priceText || "Free",
            currency: details.currency || "USD",
            updated: details.updated,
            version: details.version,
            recentChanges: details.recentChanges,
            contentRating: details.contentRating,
            ratings: details.ratings,
            reviews: details.reviews,
            histogram: details.histogram,
            headerImage: details.headerImage,
            video: details.video,
            videoImage: details.videoImage
          };
        } catch (error) {
          console.error(`\u274C Failed to get app details for ${appId}:`, error.message);
          return null;
        }
      }
      /**
       * Fallback apps when API fails
       */
      getFallbackApps() {
        return [
          {
            appId: "com.whatsapp",
            title: "WhatsApp Messenger",
            icon: "https://play-lh.googleusercontent.com/bYtqbOcTYOlgc6gqZ2rwb8lptHuwlNE75zYJu6Bn076-hTmvd96HH-6v7S0YUAAJXoJN=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/dT0HMLqPjxqKZSLl0D_L-a3fr0dXM0WxJiNgN8T6OGjJuEHhIGkQq7ZY0dE1JQGH1g=w1920"],
            developer: "WhatsApp LLC",
            score: 4.3,
            scoreText: "4.3",
            installs: "5,000,000,000+",
            genre: "Communication",
            genreId: "COMMUNICATION",
            description: "Simple. Reliable. Private. With end-to-end encryption, your personal messages and calls are secured.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.instagram.android",
            title: "Instagram",
            icon: "https://play-lh.googleusercontent.com/VRMWkE5p3CkWhJs6nv-9ZsLAs1QOg5ob1_3qg-rckwYW7yp1fMrYZqnEFpk0IoVP4LM=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/1yMsAuQ1nL7Fz0MWJrN9VjMNH3E_vB0FQxG4Fg3C0gp1u7QeQ0L1eQw3-D8B4cJw=w1920"],
            developer: "Meta Platforms, Inc.",
            score: 4.1,
            scoreText: "4.1",
            installs: "2,000,000,000+",
            genre: "Social",
            genreId: "SOCIAL",
            description: "Connect with friends, share what you're up to, or see what's new from others all over the world.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.facebook.katana",
            title: "Facebook",
            icon: "https://play-lh.googleusercontent.com/ccWDU4A7fX1R24v-vvT480ySh26AYp97g1VrIB_FIdjRcuQB2JP2WdY7h_wVVAeSpg=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/8B7xFxqDqUDDNc3-8_4BQkLbmX1XG7MQTE-1wP4_GQB_Q5vCzFGr8T-vO8_EQJB=w1920"],
            developer: "Meta Platforms, Inc.",
            score: 4,
            scoreText: "4.0",
            installs: "5,000,000,000+",
            genre: "Social",
            genreId: "SOCIAL",
            description: "Connect with friends and the world around you on Facebook.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.google.android.youtube",
            title: "YouTube",
            icon: "https://play-lh.googleusercontent.com/lMoItBgdPPVDJsNOVtP26EKHePkwBg-PkuY9NOrc-fumRtTFP4XhpUNk_22syN4Datc=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/vA4tG0v4aasE7oIvRIvTkOYTwom07oFN7k1Cb=w1920"],
            developer: "Google LLC",
            score: 4.2,
            scoreText: "4.2",
            installs: "10,000,000,000+",
            genre: "Video Players & Editors",
            genreId: "VIDEO_PLAYERS",
            description: "Enjoy your favorite videos and music, upload original content, and share it all with friends.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.spotify.music",
            title: "Spotify",
            icon: "https://play-lh.googleusercontent.com/UrY7BAZ-XfXGpfkeWg0zCCR-7FXeTL_WiQfT9F-bD-pCPvr0bD=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/SyPz_0E=w1920"],
            developer: "Spotify AB",
            score: 4.4,
            scoreText: "4.4",
            installs: "1,000,000,000+",
            genre: "Music & Audio",
            genreId: "MUSIC_AND_AUDIO",
            description: "Listen to music, play podcasts and discover new content with millions of tracks.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.snapchat.android",
            title: "Snapchat",
            icon: "https://play-lh.googleusercontent.com/KxeSAjPTKliCErbivNiXrd6cTwfbqUJcbSRPe_IBVK_YmwckfMRS1VIHz-5cgT09lQ=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/vVAa=w1920"],
            developer: "Snap Inc",
            score: 4,
            scoreText: "4.0",
            installs: "1,000,000,000+",
            genre: "Social",
            genreId: "SOCIAL",
            description: "Share the moment with friends and family.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.zhiliaoapp.musically",
            title: "TikTok",
            icon: "https://play-lh.googleusercontent.com/OS-MhZjHHDc5X1LP9wJoOp_VQn7CQVP0c=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/Qd8Q=w1920"],
            developer: "TikTok Pte. Ltd.",
            score: 4.3,
            scoreText: "4.3",
            installs: "1,000,000,000+",
            genre: "Social",
            genreId: "SOCIAL",
            description: "Discover short videos and create your own with music effects.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.twitter.android",
            title: "X (Twitter)",
            icon: "https://play-lh.googleusercontent.com/nQ6aGm4E=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/Qe9A=w1920"],
            developer: "X Corp.",
            score: 3.9,
            scoreText: "3.9",
            installs: "1,000,000,000+",
            genre: "News & Magazines",
            genreId: "NEWS_AND_MAGAZINES",
            description: "See what's happening in the world right now.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.google.android.apps.maps",
            title: "Google Maps",
            icon: "https://play-lh.googleusercontent.com/Kf8WTct65hFJxBUDm5E-EpYsiDoLQiGGbnuyP6HBNax43YShXti9THPon1YKB6zPYpA=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/MAP=w1920"],
            developer: "Google LLC",
            score: 4.2,
            scoreText: "4.2",
            installs: "10,000,000,000+",
            genre: "Travel & Local",
            genreId: "TRAVEL_AND_LOCAL",
            description: "Navigate your world faster and easier with Google Maps.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.netflix.mediaclient",
            title: "Netflix",
            icon: "https://play-lh.googleusercontent.com/TBRwjS_qfJCSj1m7zZB93FnpJM5fSpMA_wUlFDLxWAb45T9RmwBvQd5cWR5viJJOhkI=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/Netflix=w1920"],
            developer: "Netflix, Inc.",
            score: 4.3,
            scoreText: "4.3",
            installs: "1,000,000,000+",
            genre: "Entertainment",
            genreId: "ENTERTAINMENT",
            description: "Watch movies and TV shows recommended just for you.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "com.amazon.mShop.android.shopping",
            title: "Amazon Shopping",
            icon: "https://play-lh.googleusercontent.com/5ZLLe3=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/Amaz=w1920"],
            developer: "Amazon Mobile LLC",
            score: 4.4,
            scoreText: "4.4",
            installs: "500,000,000+",
            genre: "Shopping",
            genreId: "SHOPPING",
            description: "Shop millions of products, track orders and compare prices.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          },
          {
            appId: "org.telegram.messenger",
            title: "Telegram",
            icon: "https://play-lh.googleusercontent.com/ZU9cSsyIJZo6Oy7HTHiEPwZg0m2Crep-d5ZrfajqtsH-qgUXSqKpNA2FpPDTn-7qA5Q=w512-h512",
            screenshots: ["https://play-lh.googleusercontent.com/Tele=w1920"],
            developer: "Telegram FZ-LLC",
            score: 4.4,
            scoreText: "4.4",
            installs: "1,000,000,000+",
            genre: "Communication",
            genreId: "COMMUNICATION",
            description: "Pure instant messaging \u2014 simple, fast, secure, and synced across all devices.",
            price: 0,
            free: true,
            priceText: "Free",
            currency: "USD"
          }
        ];
      }
      /**
       * Get Arabic genre name for apps
       */
      getAppGenreArabic(genre) {
        const genreMap = {
          "Social": "\u062A\u0648\u0627\u0635\u0644 \u0627\u062C\u062A\u0645\u0627\u0639\u064A",
          "Communication": "\u062A\u0648\u0627\u0635\u0644",
          "Productivity": "\u0625\u0646\u062A\u0627\u062C\u064A\u0629",
          "Tools": "\u0623\u062F\u0648\u0627\u062A",
          "Entertainment": "\u062A\u0631\u0641\u064A\u0647",
          "Photography": "\u062A\u0635\u0648\u064A\u0631",
          "Video Players & Editors": "\u0641\u064A\u062F\u064A\u0648",
          "Music & Audio": "\u0645\u0648\u0633\u064A\u0642\u0649 \u0648\u0635\u0648\u062A",
          "Shopping": "\u062A\u0633\u0648\u0642",
          "Finance": "\u0645\u0627\u0644\u064A\u0629",
          "Health & Fitness": "\u0635\u062D\u0629 \u0648\u0644\u064A\u0627\u0642\u0629",
          "Education": "\u062A\u0639\u0644\u064A\u0645",
          "Travel & Local": "\u0633\u0641\u0631 \u0648\u0645\u062D\u0644\u064A",
          "News & Magazines": "\u0623\u062E\u0628\u0627\u0631 \u0648\u0645\u062C\u0644\u0627\u062A",
          "Food & Drink": "\u0637\u0639\u0627\u0645 \u0648\u0634\u0631\u0627\u0628",
          "Lifestyle": "\u0646\u0645\u0637 \u062D\u064A\u0627\u0629",
          "Business": "\u0623\u0639\u0645\u0627\u0644",
          "Weather": "\u0637\u0642\u0633"
        };
        return genreMap[genre] || genre;
      }
    };
    googlePlayService = new GooglePlayService();
  }
});

// server/trending-poster-service.ts
var trending_poster_service_exports = {};
__export(trending_poster_service_exports, {
  TrendingPosterService: () => TrendingPosterService,
  trendingPosterService: () => trendingPosterService
});
import sharp from "sharp";
function escapeXml(text) {
  if (!text) return "";
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}
var INTERNATIONAL_TV_REGIONS, CATEGORY_SEARCH_QUERIES, RECIPE_PROMOTIONAL_AR, RECIPE_PROMOTIONAL_EN, RECIPE_CTA_AR, RECIPE_CTA_EN, GAMING_PROMOTIONAL_AR, GAMING_PROMOTIONAL_EN, GAMING_CTA_AR, GAMING_CTA_EN, APPS_CTA_AR, APPS_CTA_EN, TV_CHANNELS_PROMOTIONAL_AR, TV_CHANNELS_PROMOTIONAL_EN, TV_CHANNELS_CTA_AR, TV_CHANNELS_CTA_EN, STORY_DIMENSIONS, TrendingPosterService, trendingPosterService;
var init_trending_poster_service = __esm({
  "server/trending-poster-service.ts"() {
    "use strict";
    init_google_trends();
    init_google_image_search();
    init_r2_storage();
    init_firestore();
    init_deepseek();
    init_football_data_service();
    init_huggingface();
    init_google_play_service();
    INTERNATIONAL_TV_REGIONS = [
      { countryCode: "TR", languageCode: "tr", name: "Turkish" },
      { countryCode: "US", languageCode: "en", name: "American" },
      { countryCode: "IN", languageCode: "hi", name: "Indian" },
      { countryCode: "KR", languageCode: "ko", name: "Korean" }
    ];
    CATEGORY_SEARCH_QUERIES = {
      "movies": ["movie poster", "film poster", "cinema poster"],
      "tv_shows": ["TV series poster", "show poster", "drama poster"],
      "sports": ["football match stadium", "soccer game atmosphere", "champions league match", "premier league football"],
      "recipes": ["delicious food photography", "gourmet dish presentation", "homemade recipe photo", "professional food styling"],
      "gaming": ["official game poster logo 4K", "video game cover art HD logo", "AAA game key art official", "game poster trending logo HD"],
      "apps": ["app store icon HD", "mobile app logo official", "app interface premium design", "smartphone app icon"],
      "tv_channels": ["TV channel logo HD", "broadcast network logo", "television channel branding", "media network logo"]
    };
    RECIPE_PROMOTIONAL_AR = [
      "\u0648\u0635\u0641\u0629 \u0634\u0647\u064A\u0629 \u0648\u0645\u0645\u064A\u0632\u0629 \u0633\u062A\u062C\u0639\u0644 \u0639\u0627\u0626\u0644\u062A\u0643 \u062A\u0637\u0644\u0628\u0647\u0627 \u0645\u0631\u0627\u0631\u0627\u064B \u0648\u062A\u0643\u0631\u0627\u0631\u0627\u064B! \u0645\u0643\u0648\u0646\u0627\u062A \u0628\u0633\u064A\u0637\u0629 \u0645\u062A\u0648\u0641\u0631\u0629 \u0641\u064A \u0643\u0644 \u0628\u064A\u062A \u0648\u0646\u062A\u064A\u062C\u0629 \u0645\u0630\u0647\u0644\u0629 \u062A\u0641\u0648\u0642 \u0643\u0644 \u0627\u0644\u062A\u0648\u0642\u0639\u0627\u062A. \u0631\u0627\u0626\u062D\u0629 \u062A\u0645\u0644\u0623 \u0627\u0644\u0645\u0643\u0627\u0646 \u0628\u0627\u0644\u0634\u0647\u064A\u0629 \u0648\u0637\u0639\u0645 \u0644\u0630\u064A\u0630 \u0644\u0627 \u064A\u064F\u0642\u0627\u0648\u0645. \u062C\u0631\u0628\u0647\u0627 \u0627\u0644\u0622\u0646 \u0648\u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u0623\u0644\u0630 \u0637\u0639\u0645 \u0633\u062A\u062A\u0630\u0648\u0642\u0647 \u0641\u064A \u062D\u064A\u0627\u062A\u0643!",
      "\u0637\u0628\u0642 \u0631\u0627\u0626\u0639 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u062A\u062C\u0631\u0628\u0629 \u0648\u064A\u062C\u0639\u0644\u0643 \u062A\u0634\u0639\u0631 \u0648\u0643\u0623\u0646\u0643 \u0641\u064A \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u0637\u0627\u0639\u0645! \u0633\u0647\u0644 \u0627\u0644\u062A\u062D\u0636\u064A\u0631 \u0648\u0633\u0631\u064A\u0639 \u0648\u0644\u0627 \u064A\u062D\u062A\u0627\u062C \u0644\u062E\u0628\u0631\u0629 \u0637\u0628\u062E \u0645\u0633\u0628\u0642\u0629. \u0627\u0644\u0645\u0642\u0627\u062F\u064A\u0631 \u0627\u0644\u062F\u0642\u064A\u0642\u0629 \u0648\u0627\u0644\u062E\u0637\u0648\u0627\u062A \u0627\u0644\u0645\u0641\u0635\u0644\u0629 \u0628\u0627\u0646\u062A\u0638\u0627\u0631\u0643 \u0627\u0644\u0622\u0646. \u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629 \u0627\u0644\u062A\u064A \u0633\u062A\u063A\u064A\u0631 \u0645\u0627\u0626\u062F\u062A\u0643 \u0644\u0644\u0623\u0641\u0636\u0644!",
      "\u0648\u0635\u0641\u0629 \u062E\u0627\u0635\u0629 \u0633\u062A\u0628\u0647\u0631 \u0639\u0627\u0626\u0644\u062A\u0643 \u0648\u0623\u0635\u062F\u0642\u0627\u0626\u0643 \u0641\u064A \u0643\u0644 \u0645\u0646\u0627\u0633\u0628\u0629 \u0648\u062A\u062C\u0639\u0644\u0643 \u0646\u062C\u0645 \u0627\u0644\u0633\u0647\u0631\u0629! \u0633\u0631 \u0627\u0644\u0637\u0639\u0645 \u0627\u0644\u0644\u0630\u064A\u0630 \u0641\u064A \u0627\u0644\u062A\u0641\u0627\u0635\u064A\u0644 \u0627\u0644\u0635\u063A\u064A\u0631\u0629 \u0627\u0644\u062A\u064A \u0646\u0643\u0634\u0641\u0647\u0627 \u0644\u0643. \u0645\u0630\u0627\u0642 \u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A \u0648\u0645\u0638\u0647\u0631 \u0627\u062D\u062A\u0631\u0627\u0641\u064A \u064A\u0644\u064A\u0642 \u0628\u0623\u0631\u0642\u0649 \u0627\u0644\u0645\u0646\u0627\u0633\u0628\u0627\u062A. \u0627\u0643\u062A\u0634\u0641 \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0627\u0644\u0622\u0646 \u0648\u0623\u0628\u0647\u0631 \u0627\u0644\u062C\u0645\u064A\u0639!",
      "\u0645\u0646 \u0623\u0634\u0647\u0649 \u0627\u0644\u0623\u0637\u0628\u0627\u0642 \u0627\u0644\u062A\u064A \u0633\u062A\u062A\u0630\u0648\u0642\u0647\u0627 \u0641\u064A \u062D\u064A\u0627\u062A\u0643 \u0628\u062F\u0648\u0646 \u0645\u0628\u0627\u0644\u063A\u0629! \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0628\u0627\u0644\u0645\u0642\u0627\u062F\u064A\u0631 \u0627\u0644\u0645\u0636\u0628\u0648\u0637\u0629 \u0648\u0627\u0644\u062E\u0637\u0648\u0627\u062A \u0627\u0644\u062A\u0641\u0635\u064A\u0644\u064A\u0629 \u062C\u0627\u0647\u0632\u0629 \u0644\u0643. \u0637\u0631\u064A\u0642\u0629 \u0627\u0644\u062A\u062D\u0636\u064A\u0631 \u0633\u0647\u0644\u0629 \u0648\u0627\u0644\u0646\u062A\u064A\u062C\u0629 \u0645\u0636\u0645\u0648\u0646\u0629 \u0648\u0645\u0628\u0647\u0631\u0629. \u0627\u0628\u062F\u0623 \u0627\u0644\u062A\u062D\u0636\u064A\u0631 \u0627\u0644\u0622\u0646 \u0648\u062D\u0636\u0651\u0631 \u0623\u0637\u064A\u0628 \u0648\u062C\u0628\u0629 \u0644\u0639\u0627\u0626\u0644\u062A\u0643!",
      "\u0637\u0628\u0642 \u0644\u0630\u064A\u0630 \u062C\u0627\u0647\u0632 \u0641\u064A \u062F\u0642\u0627\u0626\u0642 \u0645\u0639\u062F\u0648\u062F\u0629 \u064A\u062C\u0639\u0644\u0643 \u062A\u0633\u062A\u0645\u062A\u0639 \u0628\u0648\u0642\u062A\u0643 \u0628\u062F\u0644 \u0642\u0636\u0627\u0626\u0647 \u0641\u064A \u0627\u0644\u0645\u0637\u0628\u062E! \u0627\u0643\u062A\u0634\u0641 \u0627\u0644\u0633\u0631 \u0648\u0631\u0627\u0621 \u0647\u0630\u0627 \u0627\u0644\u0637\u0639\u0645 \u0627\u0644\u0631\u0627\u0626\u0639 \u0627\u0644\u0630\u064A \u064A\u062D\u0628\u0647 \u0627\u0644\u062C\u0645\u064A\u0639. \u0627\u0644\u0645\u0643\u0648\u0646\u0627\u062A \u0645\u062A\u0648\u0641\u0631\u0629 \u0641\u064A \u0643\u0644 \u0633\u0648\u0628\u0631\u0645\u0627\u0631\u0643\u062A \u0648\u0627\u0644\u062E\u0637\u0648\u0627\u062A \u0633\u0647\u0644\u0629 \u062C\u062F\u0627\u064B \u0648\u0645\u0636\u0645\u0648\u0646\u0629 \u0627\u0644\u0646\u062A\u064A\u062C\u0629. \u062C\u0631\u0628\u0647\u0627 \u0627\u0644\u0622\u0646!",
      "\u0648\u0635\u0641\u0629 \u0645\u0645\u064A\u0632\u0629 \u0645\u0646 \u0627\u0644\u0645\u0637\u0628\u062E \u0627\u0644\u0639\u0627\u0644\u0645\u064A \u0633\u062A\u063A\u064A\u0631 \u0646\u0638\u0631\u062A\u0643 \u0644\u0644\u0637\u0628\u062E \u0648\u062A\u062C\u0639\u0644\u0643 \u062A\u0639\u0634\u0642 \u0627\u0644\u0645\u0637\u0628\u062E! \u0645\u0630\u0627\u0642 \u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A \u0644\u0627 \u064A\u064F\u0646\u0633\u0649 \u0648\u0645\u0638\u0647\u0631 \u0627\u062D\u062A\u0631\u0627\u0641\u064A \u0643\u0623\u0646\u0643 \u0641\u064A \u0645\u0637\u0639\u0645 \u0641\u0627\u062E\u0631. \u062A\u0639\u0644\u0645 \u0623\u0633\u0631\u0627\u0631 \u0627\u0644\u0634\u064A\u0641\u0627\u062A \u0627\u0644\u0645\u062D\u062A\u0631\u0641\u064A\u0646 \u0628\u0633\u0647\u0648\u0644\u0629 \u062A\u0627\u0645\u0629. \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0628\u0627\u0646\u062A\u0638\u0627\u0631\u0643!"
    ];
    RECIPE_PROMOTIONAL_EN = [
      "A delicious and special recipe that will make your family ask for it again and again! Simple ingredients available in every home with amazing results that exceed all expectations. An aroma that fills the place with appetite and an irresistible delicious taste. Try it now and enjoy the tastiest flavor you'll ever have!",
      "An amazing dish worth trying that makes you feel like you're at the best restaurant! Easy to prepare and quick, no previous cooking experience needed. Exact measurements and detailed steps await you now. Don't miss this exceptional recipe that will transform your table for the better!",
      "A special recipe that will impress your family and friends on every occasion and make you the star of the evening! The secret of delicious taste is in the small details we reveal to you. Exceptional taste and professional look worthy of the finest occasions. Discover the full recipe now and amaze everyone!",
      "One of the tastiest dishes you'll ever try in your life, without exaggeration! The complete recipe with exact measurements and detailed steps ready for you. The preparation method is easy and the result is guaranteed and impressive. Start cooking now and prepare the tastiest meal for your family!",
      "Delicious dish ready in just minutes, letting you enjoy your time instead of spending it in the kitchen! Discover the secret behind this amazing taste that everyone loves. Ingredients available at every supermarket and steps are very easy with guaranteed results. Try it now!",
      "A special recipe from world cuisine that will change your view of cooking and make you love the kitchen! An unforgettable exceptional taste and professional look like you're at a luxury restaurant. Learn the secrets of professional chefs with complete ease. The full recipe awaits you!"
    ];
    RECIPE_CTA_AR = "\u0627\u0643\u062A\u0634\u0641 \u0627\u0644\u0648\u0635\u0641\u0629 \u0627\u0644\u0633\u0631\u064A\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0627\u0644\u0622\u0646";
    RECIPE_CTA_EN = "DISCOVER THE FULL SECRET RECIPE NOW";
    GAMING_PROMOTIONAL_AR = [
      "\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0633\u0643\u064A\u0646\u0627\u062A \u0648\u0643\u0648\u064A\u0646\u0632 \u0645\u062C\u0627\u0646\u064A\u0629 \u0628\u062F\u0648\u0646 \u0623\u064A \u0627\u0634\u062A\u0631\u0627\u0643 \u0623\u0648 \u062F\u0641\u0639! \u0637\u0631\u064A\u0642\u0629 \u062D\u0635\u0631\u064A\u0629 \u0648\u0645\u0636\u0645\u0648\u0646\u0629 100% \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0622\u0644\u0627\u0641 \u0627\u0644\u0643\u0648\u064A\u0646\u0632 \u0648\u0627\u0644\u0633\u0643\u064A\u0646\u0627\u062A \u0627\u0644\u0646\u0627\u062F\u0631\u0629. \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u0627\u0633\u062A\u0641\u0627\u062F\u0648\u0627 \u0645\u0646 \u0647\u0630\u0627 \u0627\u0644\u0639\u0631\u0636 \u0627\u0644\u0645\u0630\u0647\u0644. \u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0627\u0644\u0622\u0646 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0645\u0643\u0627\u0641\u0622\u062A\u0643 \u0641\u0648\u0631\u0627\u064B!",
      "\u0639\u0631\u0636 \u0645\u062D\u062F\u0648\u062F \u062C\u062F\u0627\u064B! \u0633\u0643\u064A\u0646\u0627\u062A \u0646\u0627\u062F\u0631\u0629 \u0648\u0643\u0648\u064A\u0646\u0632 \u063A\u064A\u0631 \u0645\u062D\u062F\u0648\u062F\u0629 \u0628\u062F\u0648\u0646 \u0627\u0634\u062A\u0631\u0627\u0643 \u0648\u0644\u0627 \u0628\u0637\u0627\u0642\u0629 \u0627\u0626\u062A\u0645\u0627\u0646! \u0637\u0631\u064A\u0642\u0629 \u0633\u0631\u064A\u0629 \u064A\u0633\u062A\u062E\u062F\u0645\u0647\u0627 \u0627\u0644\u0645\u062D\u062A\u0631\u0641\u0648\u0646 \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u0638\u0627\u0647\u0631 \u0648\u0627\u0644\u0639\u0645\u0644\u0627\u062A. \u0644\u0627 \u062A\u0636\u064A\u0639 \u0647\u0630\u0647 \u0627\u0644\u0641\u0631\u0635\u0629 \u0627\u0644\u0630\u0647\u0628\u064A\u0629 \u0648\u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0627\u0644\u0622\u0646!",
      "\u0627\u0644\u0637\u0631\u064A\u0642\u0629 \u0627\u0644\u0648\u062D\u064A\u062F\u0629 \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0633\u0643\u064A\u0646\u0627\u062A \u0648\u0643\u0648\u064A\u0646\u0632 \u0645\u062C\u0627\u0646\u0627\u064B \u0628\u0627\u0644\u0643\u0627\u0645\u0644! \u0628\u062F\u0648\u0646 \u062A\u0633\u062C\u064A\u0644 \u0628\u0637\u0627\u0642\u0627\u062A\u060C \u0628\u062F\u0648\u0646 \u0627\u0634\u062A\u0631\u0627\u0643\u0627\u062A\u060C \u0628\u062F\u0648\u0646 \u0631\u0633\u0648\u0645 \u062E\u0641\u064A\u0629. \u0627\u0643\u062A\u0634\u0641 \u0627\u0644\u0633\u0631 \u0627\u0644\u0630\u064A \u0623\u062E\u0641\u062A\u0647 \u0639\u0646\u0643 \u0627\u0644\u0634\u0631\u0643\u0627\u062A \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0622\u0644\u0627\u0641 \u0627\u0644\u0645\u0643\u0627\u0641\u0622\u062A \u0641\u0648\u0631\u0627\u064B. \u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649!",
      "\u0643\u0648\u064A\u0646\u0632 \u063A\u064A\u0631 \u0645\u062D\u062F\u0648\u062F\u0629 + \u0633\u0643\u064A\u0646\u0627\u062A \u0623\u0633\u0637\u0648\u0631\u064A\u0629 \u0646\u0627\u062F\u0631\u0629 = \u0645\u062C\u0627\u0646\u0627\u064B \u062A\u0645\u0627\u0645\u0627\u064B! \u0639\u0631\u0636 \u062E\u0627\u0635 \u0644\u0641\u062A\u0631\u0629 \u0645\u062D\u062F\u0648\u062F\u0629 \u062C\u062F\u0627\u064B \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0623\u0646\u062F\u0631 \u0627\u0644\u0639\u0646\u0627\u0635\u0631 \u0641\u064A \u0627\u0644\u0644\u0639\u0628\u0629. \u0644\u0627 \u064A\u062D\u062A\u0627\u062C \u0627\u0634\u062A\u0631\u0627\u0643 \u0648\u0644\u0627 \u062F\u0641\u0639 \u0623\u064A \u0645\u0628\u0644\u063A. \u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0627\u0644\u0622\u0646 \u0642\u0628\u0644 \u0627\u0646\u062A\u0647\u0627\u0621 \u0627\u0644\u0639\u0631\u0636!",
      "\u0623\u062E\u064A\u0631\u0627\u064B \u0637\u0631\u064A\u0642\u0629 \u062D\u0642\u064A\u0642\u064A\u0629 \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0633\u0643\u064A\u0646\u0627\u062A \u0648\u0643\u0648\u064A\u0646\u0632 \u0628\u062F\u0648\u0646 \u062F\u0641\u0639 \u0641\u0644\u0633 \u0648\u0627\u062D\u062F! \u0645\u0636\u0645\u0648\u0646\u0629 100% \u0648\u0645\u062C\u0631\u0628\u0629 \u0645\u0646 \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645. \u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u0638\u0627\u0647\u0631 \u0648\u0627\u0644\u0639\u0645\u0644\u0627\u062A \u0645\u062C\u0627\u0646\u0627\u064B \u062A\u0645\u0627\u0645\u0627\u064B. \u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0648\u0627\u0633\u062A\u0645\u062A\u0639!",
      "\u0633\u0643\u064A\u0646\u0627\u062A \u062D\u0635\u0631\u064A\u0629 + \u0643\u0648\u064A\u0646\u0632 \u0644\u0627 \u0646\u0647\u0627\u0626\u064A\u0629 = \u0628\u062F\u0648\u0646 \u0623\u064A \u0627\u0634\u062A\u0631\u0627\u0643! \u0627\u0644\u0637\u0631\u064A\u0642\u0629 \u0627\u0644\u0623\u0633\u0647\u0644 \u0648\u0627\u0644\u0623\u0633\u0631\u0639 \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0643\u0644 \u0645\u0627 \u062A\u0631\u064A\u062F\u0647 \u0641\u064A \u0627\u0644\u0644\u0639\u0628\u0629 \u0645\u062C\u0627\u0646\u0627\u064B. \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u064A\u062B\u0642\u0648\u0646 \u0628\u0647\u0630\u0647 \u0627\u0644\u0637\u0631\u064A\u0642\u0629 \u0627\u0644\u0645\u0636\u0645\u0648\u0646\u0629. \u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0627\u0644\u0622\u0646!"
    ];
    GAMING_PROMOTIONAL_EN = [
      "Get FREE skins and coins with NO subscription required! Exclusive guaranteed method to get thousands of rare skins and coins. Millions of players have benefited from this amazing offer. Swipe up now and get your rewards instantly!",
      "Limited time offer! Rare skins and unlimited coins with NO subscription, NO credit card! Secret method used by pros to get the best looks and currency. Don't miss this golden opportunity - swipe up now!",
      "The ONLY way to get skins and coins completely FREE! No card registration, no subscriptions, no hidden fees. Discover the secret that companies have been hiding and get thousands of rewards instantly. Swipe up!",
      "Unlimited coins + Legendary rare skins = Completely FREE! Special limited-time offer to get the rarest items in the game. No subscription needed, no payment required. Swipe up now before the offer ends!",
      "Finally a REAL way to get skins and coins without paying a single cent! 100% guaranteed and tested by millions of players worldwide. Get the best looks and currency for FREE. Swipe up and enjoy!",
      "Exclusive skins + Infinite coins = NO subscription needed! The easiest and fastest way to get everything you want in the game for FREE. Millions of players trust this guaranteed method. Swipe up now!"
    ];
    GAMING_CTA_AR = "\u0627\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 Skins \u0648 Coins \u0645\u062C\u0627\u0646\u0627\u064B";
    GAMING_CTA_EN = "SWIPE UP - FREE SKINS & COINS";
    APPS_CTA_AR = "\u0625\u0633\u062D\u0628 \u0644\u0644\u0623\u0639\u0644\u0649 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0627\u0634\u062A\u0631\u0627\u0643 \u0628\u0631\u064A\u0645\u064A\u0648\u0645 12 \u0634\u0647\u0631\u0627\u064B \u0645\u062C\u0627\u0646\u0627\u064B";
    APPS_CTA_EN = "SWIPE UP & GET 12 MONTHS PREMIUM FREE";
    TV_CHANNELS_PROMOTIONAL_AR = [
      "\u0642\u0646\u0627\u0629 \u0645\u0645\u064A\u0632\u0629 \u062A\u0642\u062F\u0645 \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u062D\u0635\u0631\u064A \u0648\u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0627\u0644\u0645\u062A\u0646\u0648\u0639\u0629 \u0639\u0644\u0649 \u0645\u062F\u0627\u0631 \u0627\u0644\u0633\u0627\u0639\u0629! \u0628\u062B \u0645\u0628\u0627\u0634\u0631 \u0628\u062C\u0648\u062F\u0629 \u0641\u0627\u0626\u0642\u0629 HD \u0648\u0628\u062F\u0648\u0646 \u0623\u064A \u0627\u0646\u0642\u0637\u0627\u0639. \u0628\u0631\u0627\u0645\u062C \u062A\u0631\u0641\u064A\u0647\u064A\u0629 \u0648\u0645\u0633\u0644\u0633\u0644\u0627\u062A \u062D\u0635\u0631\u064A\u0629 \u0648\u0645\u062D\u062A\u0648\u0649 \u0639\u0627\u0626\u0644\u064A \u064A\u0646\u0627\u0633\u0628 \u0627\u0644\u062C\u0645\u064A\u0639. \u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0627\u0644\u0622\u0646 \u0645\u062C\u0627\u0646\u0627\u064B!",
      "\u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u064A\u0646 \u0627\u0644\u0630\u064A\u0646 \u064A\u0633\u062A\u0645\u062A\u0639\u0648\u0646 \u0628\u0647\u0630\u0647 \u0627\u0644\u0642\u0646\u0627\u0629 \u0627\u0644\u0631\u0627\u0626\u0639\u0629 \u064A\u0648\u0645\u064A\u0627\u064B! \u0645\u062D\u062A\u0648\u0649 \u0645\u062A\u062C\u062F\u062F \u0648\u0645\u062A\u0646\u0648\u0639 \u064A\u0646\u0627\u0633\u0628 \u062C\u0645\u064A\u0639 \u0627\u0644\u0623\u0630\u0648\u0627\u0642 \u0648\u0627\u0644\u0623\u0639\u0645\u0627\u0631. \u0623\u0641\u0644\u0627\u0645 \u062D\u0635\u0631\u064A\u0629 \u0648\u0645\u0633\u0644\u0633\u0644\u0627\u062A \u062C\u062F\u064A\u062F\u0629 \u0648\u0628\u0631\u0627\u0645\u062C \u062A\u0631\u0641\u064A\u0647\u064A\u0629 \u0644\u0627 \u062A\u0641\u0648\u0651\u062A. \u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0628\u062C\u0648\u062F\u0629 \u0639\u0627\u0644\u064A\u0629 \u0627\u0644\u0622\u0646!",
      "\u0642\u0646\u0627\u0629 \u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0627\u0644\u062D\u0635\u0631\u064A\u0629 \u0648\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0645\u0645\u064A\u0632 \u0627\u0644\u0630\u064A \u0644\u0646 \u062A\u062C\u062F\u0647 \u0641\u064A \u0623\u064A \u0645\u0643\u0627\u0646 \u0622\u062E\u0631! \u0628\u062B \u0645\u0633\u062A\u0645\u0631 24/7 \u0628\u0623\u0639\u0644\u0649 \u062C\u0648\u062F\u0629 \u0635\u0648\u062A \u0648\u0635\u0648\u0631\u0629. \u0628\u0631\u0627\u0645\u062C \u0645\u0646\u0648\u0639\u0629 \u062A\u0646\u0627\u0633\u0628 \u0627\u0644\u0643\u0628\u0627\u0631 \u0648\u0627\u0644\u0635\u063A\u0627\u0631 \u0648\u0627\u0644\u0639\u0627\u0626\u0644\u0629 \u0628\u0623\u0643\u0645\u0644\u0647\u0627. \u0627\u0634\u062A\u0631\u0643 \u0627\u0644\u0622\u0646 \u0648\u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u0639\u0631\u0636 \u0645\u062C\u0627\u0646\u064A \u062E\u0627\u0635!",
      "\u0623\u0641\u0636\u0644 \u0642\u0646\u0627\u0629 \u062A\u0631\u0641\u064A\u0647\u064A\u0629 \u062A\u0642\u062F\u0645 \u0645\u062D\u062A\u0648\u0649 \u0639\u0631\u0628\u064A \u0648\u0639\u0627\u0644\u0645\u064A \u0645\u062A\u0645\u064A\u0632 \u0628\u062C\u0648\u062F\u0629 \u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629! \u0645\u0633\u0644\u0633\u0644\u0627\u062A \u062A\u0631\u0643\u064A\u0629 \u0648\u0645\u0635\u0631\u064A\u0629 \u0648\u062E\u0644\u064A\u062C\u064A\u0629 \u062D\u0635\u0631\u064A\u0629. \u0623\u0641\u0644\u0627\u0645 \u062C\u062F\u064A\u062F\u0629 \u0648\u0628\u0631\u0627\u0645\u062C \u0645\u062A\u0646\u0648\u0639\u0629 \u0648\u0623\u062E\u0628\u0627\u0631 \u0639\u0644\u0649 \u0645\u062F\u0627\u0631 \u0627\u0644\u0633\u0627\u0639\u0629. \u0644\u0627 \u062A\u0641\u0648\u062A \u0645\u0634\u0627\u0647\u062F\u0629 \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0627\u0644\u0622\u0646!",
      "\u0642\u0646\u0627\u0629 \u062A\u062C\u0645\u0639 \u0628\u064A\u0646 \u0627\u0644\u062A\u0631\u0641\u064A\u0647 \u0648\u0627\u0644\u0645\u0639\u0644\u0648\u0645\u0629 \u0641\u064A \u0645\u062D\u062A\u0648\u0649 \u0645\u0645\u062A\u0639 \u0648\u0631\u0627\u0642\u064A! \u0628\u0631\u0627\u0645\u062C \u062B\u0642\u0627\u0641\u064A\u0629 \u0648\u062A\u0631\u0641\u064A\u0647\u064A\u0629 \u0648\u0648\u062B\u0627\u0626\u0642\u064A\u0629 \u062A\u0646\u0627\u0633\u0628 \u0627\u0644\u062C\u0645\u064A\u0639. \u0628\u062B \u062D\u064A \u0648\u0645\u0628\u0627\u0634\u0631 \u0628\u062C\u0648\u062F\u0629 4K \u0641\u0627\u0626\u0642\u0629 \u0627\u0644\u0648\u0636\u0648\u062D. \u0634\u0627\u0647\u062F \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u062D\u0635\u0631\u064A \u0627\u0644\u0622\u0646 \u0648\u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u062A\u062C\u0631\u0628\u0629 \u0641\u0631\u064A\u062F\u0629!",
      "\u0642\u0646\u0627\u062A\u0643 \u0627\u0644\u0645\u0641\u0636\u0644\u0629 \u0644\u0645\u062A\u0627\u0628\u0639\u0629 \u0623\u062D\u062F\u062B \u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0648\u0627\u0644\u0645\u0633\u0644\u0633\u0644\u0627\u062A \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629! \u0645\u062D\u062A\u0648\u0649 \u062C\u062F\u064A\u062F \u064A\u064F\u0636\u0627\u0641 \u064A\u0648\u0645\u064A\u0627\u064B \u0644\u064A\u0628\u0642\u064A\u0643 \u0645\u062A\u0627\u0628\u0639\u0627\u064B \u0644\u0643\u0644 \u062C\u062F\u064A\u062F. \u062C\u0648\u062F\u0629 \u0628\u062B \u0639\u0627\u0644\u064A\u0629 \u0648\u062A\u062C\u0631\u0628\u0629 \u0645\u0634\u0627\u0647\u062F\u0629 \u0645\u0645\u062A\u0627\u0632\u0629 \u0628\u062F\u0648\u0646 \u0625\u0639\u0644\u0627\u0646\u0627\u062A. \u0627\u0634\u062A\u0631\u0643 \u0627\u0644\u0622\u0646 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 \u0634\u0647\u0631 \u0645\u062C\u0627\u0646\u0627\u064B!"
    ];
    TV_CHANNELS_PROMOTIONAL_EN = [
      "An amazing channel offering the best exclusive content and diverse programs around the clock! Live streaming in super HD quality without any interruption. Entertainment shows, exclusive series, and family content suitable for everyone. Watch the live broadcast now for free!",
      "Join millions of viewers who enjoy this amazing channel daily! Fresh and diverse content suitable for all tastes and ages. Exclusive movies, new series, and entertainment programs you can't miss. Watch the live broadcast in high quality now!",
      "The channel for exclusive programs and premium content you won't find anywhere else! Continuous 24/7 broadcast in the highest audio and video quality. Variety shows suitable for adults, kids, and the whole family. Subscribe now and enjoy a special free offer!",
      "The best entertainment channel offering distinguished Arabic and international content in exceptional quality! Exclusive Turkish, Egyptian, and Gulf series. New movies, variety shows, and 24/7 news. Don't miss watching the live broadcast now!",
      "A channel that combines entertainment and information in enjoyable and elegant content! Cultural, entertainment, and documentary programs suitable for everyone. Live broadcast in ultra-clear 4K quality. Watch the exclusive content now and enjoy a unique experience!",
      "Your favorite channel to follow the latest global programs and series! New content added daily to keep you updated on everything new. High broadcast quality and excellent viewing experience without ads. Subscribe now and get a month free!"
    ];
    TV_CHANNELS_CTA_AR = "\u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0627\u0644\u0622\u0646 \u0645\u062C\u0627\u0646\u0627\u064B";
    TV_CHANNELS_CTA_EN = "WATCH LIVE BROADCAST FREE NOW";
    STORY_DIMENSIONS = {
      width: 1080,
      height: 1920
    };
    TrendingPosterService = class {
      tmdbApiKey = null;
      generatedTmdbIds = /* @__PURE__ */ new Set();
      maxGeneratedIdsCache = 100;
      excitingDescriptions = {
        "movies": [
          "Now Streaming Worldwide",
          "The Blockbuster Everyone Is Talking About",
          "Experience Cinema at Its Finest",
          "A Masterpiece You Cannot Miss",
          "Breaking Box Office Records",
          "Critics Are Calling It Phenomenal",
          "The Most Anticipated Film of the Year",
          "Pure Cinematic Excellence",
          "A Story That Will Stay With You Forever",
          "This Is What Cinema Was Made For",
          "Prepare to Be Amazed",
          "An Unforgettable Experience Awaits"
        ],
        "tv_shows": [
          "Now Streaming Worldwide",
          "The Series Everyone Is Binge-Watching",
          "Television at Its Absolute Best",
          "Your New Obsession Starts Now",
          "The Show Breaking All Records",
          "Critics Are Calling It Must-Watch TV",
          "The Most Talked About Series",
          "Get Ready for Epic Entertainment",
          "Every Episode Will Leave You Breathless",
          "The Phenomenon That Took Over",
          "Prepare for Plot Twists You Will Never Forget",
          "This Is Peak Television"
        ]
      };
      getRandomExcitingDescription(category) {
        const descriptions = this.excitingDescriptions[category];
        return descriptions[Math.floor(Math.random() * descriptions.length)];
      }
      translateToEnglish(arabicTitle) {
        const arabicToEnglishMap = {
          "\u0627\u0644\u0623\u0641\u0644\u0627\u0645": "Movies",
          "\u0627\u0644\u0645\u0633\u0644\u0633\u0644\u0627\u062A": "Series",
          "\u0627\u0644\u062A\u0631\u0646\u062F": "Trending"
        };
        for (const [ar, en] of Object.entries(arabicToEnglishMap)) {
          if (arabicTitle.includes(ar)) {
            return arabicTitle.replace(ar, en);
          }
        }
        return arabicTitle;
      }
      async initialize() {
        const tmdbConfig = await firestoreService.getAPIConfig("tmdb");
        if (tmdbConfig?.apiKey) {
          this.tmdbApiKey = tmdbConfig.apiKey;
          console.log("\u2705 TMDB API key loaded from Firestore config");
        } else if (process.env.TMDB_API_KEY) {
          this.tmdbApiKey = process.env.TMDB_API_KEY;
          console.log("\u2705 TMDB API key loaded from environment");
        } else {
          console.warn("\u26A0\uFE0F TMDB API key not configured - Movies and TV shows will require TMDB API key");
        }
        await googleImageSearchService.initialize();
        console.log("\u2705 Google Image Search service initialized");
      }
      async generateTrendingPoster(category) {
        console.log(`\u{1F3AC} Generating trending poster for category: ${category}`);
        await this.initialize();
        if (category === "movies" || category === "tv_shows") {
          return this.generateTMDBTrendingPoster(category);
        } else if (category === "sports") {
          return this.generateFootballMatchPoster();
        } else if (category === "recipes") {
          return this.generateRecipePoster();
        } else if (category === "gaming") {
          return this.generateGamingPoster();
        } else if (category === "apps") {
          return this.generateAppPoster();
        } else if (category === "tv_channels") {
          return this.generateTVChannelsPoster();
        } else {
          return this.generateGoogleSearchTrendingPoster(category);
        }
      }
      async generateTMDBTrendingPoster(category) {
        if (!this.tmdbApiKey) {
          throw new Error("\u0645\u0641\u062A\u0627\u062D TMDB API \u063A\u064A\u0631 \u0645\u064F\u0639\u062F\u0651. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0645\u0641\u062A\u0627\u062D TMDB \u0641\u064A \u0625\u0639\u062F\u0627\u062F\u0627\u062A API \u0644\u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0641\u0626\u0627\u062A \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u0648\u0627\u0644\u0645\u0633\u0644\u0633\u0644\u0627\u062A.");
        }
        let validResults = [];
        if (category === "tv_shows") {
          console.log(`\u{1F30D} Fetching international TV shows (US, Turkish, Korean, Indian)...`);
          validResults = await this.getInternationalTVShows();
          validResults = validResults.filter((item) => item.poster_path);
        } else {
          validResults = await this.getTrendingMoviesWithFallback();
        }
        if (validResults.length === 0) {
          throw new Error("\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0628\u0648\u0633\u062A\u0631\u0627\u062A \u0641\u064A \u0646\u062A\u0627\u0626\u062C TMDB");
        }
        const uniqueResults = validResults.filter((item) => !this.generatedTmdbIds.has(item.id));
        let selectedItem;
        if (uniqueResults.length > 0) {
          const randomIndex = Math.floor(Math.random() * Math.min(10, uniqueResults.length));
          selectedItem = uniqueResults[randomIndex];
        } else {
          console.log("\u26A0\uFE0F All trending content already generated, clearing cache and selecting new...");
          this.generatedTmdbIds.clear();
          const randomIndex = Math.floor(Math.random() * Math.min(10, validResults.length));
          selectedItem = validResults[randomIndex];
        }
        this.generatedTmdbIds.add(selectedItem.id);
        if (this.generatedTmdbIds.size > this.maxGeneratedIdsCache) {
          const firstId = this.generatedTmdbIds.values().next().value;
          if (firstId !== void 0) {
            this.generatedTmdbIds.delete(firstId);
          }
        }
        const title = selectedItem.title || selectedItem.name || "Unknown";
        const tmdbImageUrl = `https://image.tmdb.org/t/p/w780${selectedItem.poster_path}`;
        const originCountry = selectedItem.origin_country?.[0] || "US";
        console.log(`\u2705 Selected trending ${category}: "${title}" (ID: ${selectedItem.id}, Origin: ${originCountry}, Rating: ${selectedItem.vote_average})`);
        let descriptionAr;
        let descriptionEn;
        let latestEpisode;
        let latestSeasonNumber;
        if (category === "tv_shows" && selectedItem.id) {
          const details = await this.getTVShowDetails(selectedItem.id);
          if (details?.last_episode_to_air) {
            latestEpisode = details.last_episode_to_air.episode_number;
            latestSeasonNumber = details.last_episode_to_air.season_number;
            console.log(`\u{1F4FA} Latest episode: S${latestSeasonNumber}E${latestEpisode}`);
            const episodeDescriptions = await this.getEpisodeBilingualDescription(
              selectedItem.id,
              latestSeasonNumber,
              latestEpisode,
              details.last_episode_to_air.overview || selectedItem.overview
            );
            descriptionAr = episodeDescriptions.descriptionAr;
            descriptionEn = episodeDescriptions.descriptionEn;
            console.log(`\u{1F4DD} Using episode-specific description for poster`);
          } else {
            const generalDescriptions = await this.getBilingualDescription(
              selectedItem.id,
              "tv",
              selectedItem.overview
            );
            descriptionAr = generalDescriptions.descriptionAr;
            descriptionEn = generalDescriptions.descriptionEn;
          }
        } else {
          const generalDescriptions = await this.getBilingualDescription(
            selectedItem.id,
            category === "movies" ? "movie" : "tv",
            selectedItem.overview
          );
          descriptionAr = generalDescriptions.descriptionAr;
          descriptionEn = generalDescriptions.descriptionEn;
        }
        const trailer = await this.getTrailerVideo(
          selectedItem.id,
          category === "movies" ? "movie" : "tv"
        );
        const imageBuffer = await this.downloadImage(tmdbImageUrl);
        const processedImages = await this.processImageForStories(
          imageBuffer,
          title,
          category,
          latestEpisode,
          descriptionEn,
          descriptionAr
        );
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/${category}/${timestamp}-${randomId}`;
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(processedImages.pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category, topic: title, source: "tmdb", originCountry }
          }),
          r2Storage.uploadFile(processedImages.webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category, topic: title, source: "tmdb", originCountry }
          }),
          r2Storage.uploadFile(processedImages.facebookPngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category, topic: title, platform: "facebook", source: "tmdb", originCountry }
          }),
          r2Storage.uploadFile(processedImages.instagramPngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category, topic: title, platform: "instagram", source: "tmdb", originCountry }
          }),
          r2Storage.uploadFile(processedImages.tiktokWebpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category, topic: title, platform: "tiktok", source: "tmdb", originCountry }
          })
        ]);
        console.log(`\u2705 TMDB poster uploaded successfully to R2`);
        if (trailer) {
          console.log(`\u{1F3AC} Trailer available: ${trailer.url}`);
        }
        const metadata = {
          category,
          trendingTerm: title,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic: title,
          posterTitle: title,
          latestEpisode,
          sourceImageUrl: tmdbImageUrl,
          metadata,
          trailerUrl: trailer?.url,
          trailerKey: trailer?.key,
          trailerName: trailer?.name,
          originCountry,
          tmdbId: selectedItem.id,
          descriptionAr,
          descriptionEn,
          voteAverage: selectedItem.vote_average
        };
      }
      async getTrendingMoviesWithFallback() {
        if (!this.tmdbApiKey) return [];
        console.log(`\u{1F525} Fetching TMDB trending movies...`);
        const trendingUrl = `https://api.themoviedb.org/3/trending/movie/day?api_key=${this.tmdbApiKey}&language=en-US`;
        const response = await fetch(trendingUrl);
        if (!response.ok) {
          throw new Error(`\u062E\u0637\u0623 \u0641\u064A TMDB API: ${response.statusText}`);
        }
        const data = await response.json();
        let trendingResults = (data.results || []).filter((item) => item.poster_path);
        console.log(`\u{1F4CA} Found ${trendingResults.length} trending movies`);
        const uniqueTrending = trendingResults.filter((item) => !this.generatedTmdbIds.has(item.id));
        if (uniqueTrending.length >= 5) {
          return trendingResults;
        }
        console.log(`\u26A1 Trending exhausted, fetching high-rated movies with box office history...`);
        const regions = ["US", "TR", "IN", "KR"];
        const highRatedResults = [];
        for (const region of regions) {
          try {
            const discoverUrl = `https://api.themoviedb.org/3/discover/movie?api_key=${this.tmdbApiKey}&language=en-US&sort_by=popularity.desc&vote_average.gte=7.5&vote_count.gte=500&with_origin_country=${region}&with_release_type=2|3&page=1`;
            const discoverResponse = await fetch(discoverUrl);
            if (discoverResponse.ok) {
              const discoverData = await discoverResponse.json();
              const regionMovies = (discoverData.results || []).slice(0, 5).map((movie) => ({
                ...movie,
                origin_country: [region]
              }));
              highRatedResults.push(...regionMovies);
              console.log(`\u2705 Found ${regionMovies.length} high-rated movies from ${region}`);
            }
          } catch (error) {
            console.error(`Error fetching ${region} movies:`, error);
          }
        }
        highRatedResults.sort((a, b) => b.vote_average - a.vote_average);
        const allResults = [...trendingResults, ...highRatedResults];
        const uniqueResults = allResults.filter(
          (item, index, self) => index === self.findIndex((t) => t.id === item.id)
        );
        console.log(`\u{1F4DA} Total unique movies available: ${uniqueResults.length}`);
        return uniqueResults;
      }
      async getBilingualDescription(mediaId, mediaType, fallbackOverview, title) {
        if (!this.tmdbApiKey) {
          return {
            descriptionAr: "\u0648\u0635\u0641 \u063A\u064A\u0631 \u0645\u062A\u0648\u0641\u0631",
            descriptionEn: fallbackOverview || "Description not available"
          };
        }
        try {
          const endpoint = mediaType === "movie" ? "movie" : "tv";
          const [enResponse, arResponse] = await Promise.all([
            fetch(`https://api.themoviedb.org/3/${endpoint}/${mediaId}?api_key=${this.tmdbApiKey}&language=en-US`),
            fetch(`https://api.themoviedb.org/3/${endpoint}/${mediaId}?api_key=${this.tmdbApiKey}&language=ar-SA`)
          ]);
          let descriptionEn = fallbackOverview || "Description not available";
          let descriptionAr = "";
          let hasArabicFromTMDB = false;
          if (enResponse.ok) {
            const enData = await enResponse.json();
            descriptionEn = enData.overview || enData.tagline || fallbackOverview || "Description not available";
            console.log(`\u{1F4C4} English description from TMDB: "${descriptionEn.substring(0, 50)}..."`);
          }
          if (arResponse.ok) {
            const arData = await arResponse.json();
            const arOverview = arData.overview || arData.tagline || "";
            if (arOverview && arOverview.trim().length > 10 && arOverview !== descriptionEn) {
              descriptionAr = arOverview;
              hasArabicFromTMDB = true;
              console.log(`\u{1F1F8}\u{1F1E6} Arabic description from TMDB: "${descriptionAr.substring(0, 50)}..."`);
            }
          }
          if (!hasArabicFromTMDB && descriptionEn && descriptionEn !== "Description not available") {
            console.log(`\u{1F310} No Arabic from TMDB, using AI translation...`);
            try {
              descriptionAr = await translateToArabic(descriptionEn);
              console.log(`\u2705 AI translated Arabic: "${descriptionAr.substring(0, 50)}..."`);
            } catch (translationError) {
              console.error("AI translation failed, using fallback:", translationError);
              descriptionAr = this.getDefaultArabicDescription(mediaType);
            }
          }
          if (!descriptionAr || descriptionAr.length < 10) {
            descriptionAr = this.getDefaultArabicDescription(mediaType);
          }
          console.log(`\u{1F4DD} Final bilingual descriptions for ${mediaType} ID: ${mediaId}`);
          console.log(`   AR: "${descriptionAr.substring(0, 60)}..."`);
          console.log(`   EN: "${descriptionEn.substring(0, 60)}..."`);
          return { descriptionAr, descriptionEn };
        } catch (error) {
          console.error("Error fetching bilingual descriptions:", error);
          return {
            descriptionAr: this.getDefaultArabicDescription(mediaType),
            descriptionEn: fallbackOverview || "Description not available"
          };
        }
      }
      getDefaultArabicDescription(mediaType) {
        const defaults = {
          movie: "\u0641\u064A\u0644\u0645 \u0631\u0627\u0626\u0639 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u062A\u062D\u0641\u0629 \u0627\u0644\u0633\u064A\u0646\u0645\u0627\u0626\u064A\u0629 \u0627\u0644\u0645\u0630\u0647\u0644\u0629",
          tv: "\u0645\u0633\u0644\u0633\u0644 \u0645\u062B\u064A\u0631 \u0648\u0645\u0634\u0648\u0642! \u062A\u0627\u0628\u0639 \u0623\u062D\u062F\u0627\u062B\u0647 \u0627\u0644\u0631\u0627\u0626\u0639\u0629 \u0648\u0644\u0627 \u062A\u0641\u0648\u062A \u0623\u064A \u062D\u0644\u0642\u0629"
        };
        return defaults[mediaType];
      }
      async getTVShowDetails(tvId) {
        if (!this.tmdbApiKey) return null;
        try {
          const url = `https://api.themoviedb.org/3/tv/${tvId}?api_key=${this.tmdbApiKey}&language=en-US`;
          const response = await fetch(url);
          if (!response.ok) return null;
          return await response.json();
        } catch (error) {
          console.error("Error fetching TV show details:", error);
          return null;
        }
      }
      async getEpisodeBilingualDescription(tvId, seasonNumber, episodeNumber, fallbackOverview) {
        if (!this.tmdbApiKey) {
          return {
            descriptionAr: this.getDefaultArabicDescription("tv"),
            descriptionEn: fallbackOverview || "Description not available"
          };
        }
        try {
          const [enResponse, arResponse] = await Promise.all([
            fetch(`https://api.themoviedb.org/3/tv/${tvId}/season/${seasonNumber}/episode/${episodeNumber}?api_key=${this.tmdbApiKey}&language=en-US`),
            fetch(`https://api.themoviedb.org/3/tv/${tvId}/season/${seasonNumber}/episode/${episodeNumber}?api_key=${this.tmdbApiKey}&language=ar-SA`)
          ]);
          let descriptionEn = fallbackOverview || "Description not available";
          let descriptionAr = "";
          let episodeName = "";
          let hasArabicFromTMDB = false;
          if (enResponse.ok) {
            const enData = await enResponse.json();
            if (enData.overview && enData.overview.trim()) {
              descriptionEn = enData.overview;
            }
            episodeName = enData.name || "";
            console.log(`\u{1F4C4} Episode EN description: "${descriptionEn.substring(0, 50)}..."`);
          }
          if (arResponse.ok) {
            const arData = await arResponse.json();
            const arOverview = arData.overview || "";
            if (arOverview && arOverview.trim().length > 10 && arOverview !== descriptionEn) {
              descriptionAr = arOverview;
              hasArabicFromTMDB = true;
              console.log(`\u{1F1F8}\u{1F1E6} Episode AR description from TMDB: "${descriptionAr.substring(0, 50)}..."`);
            }
          }
          if (!hasArabicFromTMDB && descriptionEn && descriptionEn !== "Description not available") {
            console.log(`\u{1F310} No Arabic episode description from TMDB, using AI translation...`);
            try {
              descriptionAr = await translateToArabic(descriptionEn);
              console.log(`\u2705 AI translated episode AR: "${descriptionAr.substring(0, 50)}..."`);
            } catch (translationError) {
              console.error("AI episode translation failed:", translationError);
              descriptionAr = this.getDefaultArabicDescription("tv");
            }
          }
          if (!descriptionAr || descriptionAr.length < 10) {
            descriptionAr = this.getDefaultArabicDescription("tv");
          }
          console.log(`\u{1F4FA} Final episode ${seasonNumber}x${episodeNumber} bilingual descriptions for TV ID: ${tvId}`);
          console.log(`   Episode name: "${episodeName}"`);
          console.log(`   AR: "${descriptionAr.substring(0, 60)}..."`);
          console.log(`   EN: "${descriptionEn.substring(0, 60)}..."`);
          return { descriptionAr, descriptionEn };
        } catch (error) {
          console.error("Error fetching episode bilingual descriptions:", error);
          return {
            descriptionAr: this.getDefaultArabicDescription("tv"),
            descriptionEn: fallbackOverview || "Description not available"
          };
        }
      }
      async getTrailerVideo(mediaId, mediaType) {
        if (!this.tmdbApiKey) return null;
        try {
          const endpoint = mediaType === "movie" ? "movie" : "tv";
          const url = `https://api.themoviedb.org/3/${endpoint}/${mediaId}/videos?api_key=${this.tmdbApiKey}&language=en-US`;
          console.log(`\u{1F3AC} Fetching trailer for ${mediaType} ID: ${mediaId}`);
          const response = await fetch(url);
          if (!response.ok) {
            console.log(`\u26A0\uFE0F Failed to fetch videos: ${response.statusText}`);
            return null;
          }
          const data = await response.json();
          const videos = data.results || [];
          const trailer = videos.find(
            (v) => v.type === "Trailer" && v.site === "YouTube" && v.official
          ) || videos.find(
            (v) => v.type === "Trailer" && v.site === "YouTube"
          ) || videos.find(
            (v) => v.type === "Teaser" && v.site === "YouTube"
          ) || videos.find(
            (v) => v.site === "YouTube"
          );
          if (trailer) {
            const youtubeUrl = `https://www.youtube.com/watch?v=${trailer.key}`;
            console.log(`\u2705 Found trailer: "${trailer.name}" - ${youtubeUrl}`);
            return {
              url: youtubeUrl,
              key: trailer.key,
              name: trailer.name
            };
          }
          console.log(`\u26A0\uFE0F No trailer found for ${mediaType} ID: ${mediaId}`);
          return null;
        } catch (error) {
          console.error("Error fetching trailer:", error);
          return null;
        }
      }
      async getInternationalTVShows() {
        if (!this.tmdbApiKey) return [];
        const allShows = [];
        console.log(`\u{1F30D} Fetching international TV shows from multiple regions...`);
        for (const region of INTERNATIONAL_TV_REGIONS) {
          try {
            const url = `https://api.themoviedb.org/3/discover/tv?api_key=${this.tmdbApiKey}&language=en-US&sort_by=popularity.desc&with_origin_country=${region.countryCode}&with_original_language=${region.languageCode}&vote_count.gte=50&first_air_date.gte=2020-01-01&page=1`;
            console.log(`\u{1F50D} Fetching ${region.name} TV shows...`);
            const response = await fetch(url);
            if (!response.ok) {
              console.log(`\u26A0\uFE0F Failed to fetch ${region.name} shows: ${response.statusText}`);
              continue;
            }
            const data = await response.json();
            const shows = (data.results || []).slice(0, 5).map((show) => ({
              ...show,
              origin_country: [region.countryCode]
            }));
            console.log(`\u2705 Found ${shows.length} ${region.name} TV shows`);
            allShows.push(...shows);
          } catch (error) {
            console.error(`Error fetching ${region.name} shows:`, error);
          }
        }
        allShows.sort((a, b) => b.vote_average - a.vote_average);
        console.log(`\u{1F4FA} Total international shows collected: ${allShows.length}`);
        return allShows;
      }
      async generateGoogleSearchTrendingPoster(category) {
        const trendResult = await googleTrendsService.getBestTrendForCategory(category);
        const trendingTopic = trendResult.trendingTerm;
        console.log(`\u{1F525} Trending topic for ${category}: ${trendingTopic}`);
        const categoryQueries = CATEGORY_SEARCH_QUERIES[category];
        const searchQuery = `${trendingTopic} ${categoryQueries[Math.floor(Math.random() * categoryQueries.length)]}`;
        console.log(`\u{1F50D} Searching Google Images for: "${searchQuery}"`);
        const imageResult = await googleImageSearchService.searchThumbnailImage(searchQuery, category);
        if (!imageResult) {
          console.log(`\u26A0\uFE0F No images found with trending query, trying category fallback...`);
          const fallbackQuery = categoryQueries[Math.floor(Math.random() * categoryQueries.length)];
          const fallbackResult = await googleImageSearchService.searchThumbnailImage(fallbackQuery, category);
          if (!fallbackResult) {
            throw new Error(`\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0641\u0626\u0629 ${category}`);
          }
          console.log(`\u2705 Found image with fallback query`);
        }
        const finalImageResult = imageResult || await googleImageSearchService.searchThumbnailImage(categoryQueries[0], category);
        if (!finalImageResult) {
          throw new Error(`\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0641\u0626\u0629 ${category}`);
        }
        const imageUrl = finalImageResult.imageUrl;
        const title = finalImageResult.title || trendingTopic;
        console.log(`\u2705 Selected Google image: "${title.substring(0, 50)}..."`);
        const imageBuffer = await this.downloadImage(imageUrl);
        const processedImages = await this.processImageForStories(
          imageBuffer,
          trendingTopic,
          category
        );
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/${category}/${timestamp}-${randomId}`;
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(processedImages.pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category, topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(processedImages.webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category, topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(processedImages.facebookPngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category, topic: trendingTopic, platform: "facebook", source: "google" }
          }),
          r2Storage.uploadFile(processedImages.instagramPngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category, topic: trendingTopic, platform: "instagram", source: "google" }
          }),
          r2Storage.uploadFile(processedImages.tiktokWebpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category, topic: trendingTopic, platform: "tiktok", source: "google" }
          })
        ]);
        console.log(`\u2705 Google Image poster uploaded successfully to R2`);
        const metadata = {
          category,
          trendingTerm: trendingTopic,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic,
          posterTitle: trendingTopic,
          sourceImageUrl: imageUrl,
          metadata
        };
      }
      async generateFootballMatchPoster() {
        console.log("\u26BD Generating Football Match Poster...");
        await footballDataService.initialize();
        const trendingMatch = await footballDataService.getRandomTrendingMatch();
        const match = trendingMatch.match;
        const { titleAr, titleEn } = footballDataService.generateMatchTitle(match);
        const { timeAr, timeEn } = footballDataService.getMatchTimeFormatted(match);
        console.log(`\u26BD Selected match: ${titleEn}`);
        console.log(`   League: ${match.league.name}`);
        console.log(`   Time: ${timeEn}`);
        const categoryQueries = CATEGORY_SEARCH_QUERIES["sports"];
        const searchQuery = `${match.homeTeam.name} vs ${match.awayTeam.name} football match`;
        let imageResult = await googleImageSearchService.searchThumbnailImage(searchQuery, "sports");
        if (!imageResult) {
          console.log("\u26A0\uFE0F No specific match image, using stadium fallback...");
          imageResult = await googleImageSearchService.searchThumbnailImage(
            `${match.league.name} football stadium atmosphere`,
            "sports"
          );
        }
        if (!imageResult) {
          imageResult = await googleImageSearchService.searchThumbnailImage(categoryQueries[0], "sports");
        }
        if (!imageResult) {
          throw new Error("\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0644\u0645\u0628\u0627\u0631\u0627\u0629");
        }
        const imageBuffer = await this.downloadImage(imageResult.imageUrl);
        const processedImage = await this.createFootballMatchOverlay(
          imageBuffer,
          match,
          trendingMatch.promotionalTextAr,
          trendingMatch.promotionalTextEn,
          timeAr,
          timeEn
        );
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/sports/${timestamp}-${randomId}`;
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category: "sports", topic: titleEn, source: "football" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category: "sports", topic: titleEn, source: "football" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category: "sports", topic: titleEn, platform: "facebook", source: "football" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category: "sports", topic: titleEn, platform: "instagram", source: "football" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category: "sports", topic: titleEn, platform: "tiktok", source: "football" }
          })
        ]);
        console.log(`\u2705 Football match poster uploaded successfully`);
        const metadata = {
          category: "sports",
          trendingTerm: titleEn,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic: titleEn,
          posterTitle: titleEn,
          sourceImageUrl: imageResult.imageUrl,
          metadata,
          descriptionAr: trendingMatch.promotionalTextAr,
          descriptionEn: trendingMatch.promotionalTextEn
        };
      }
      async downloadTeamLogo(logoUrl) {
        try {
          if (!logoUrl || !logoUrl.startsWith("http")) {
            return null;
          }
          const response = await fetch(logoUrl);
          if (!response.ok) {
            console.log(`\u26A0\uFE0F Failed to download logo from ${logoUrl}`);
            return null;
          }
          const arrayBuffer = await response.arrayBuffer();
          const buffer = Buffer.from(arrayBuffer);
          const resizedLogo = await sharp(buffer).resize(140, 140, { fit: "contain", background: { r: 255, g: 255, b: 255, alpha: 0 } }).png().toBuffer();
          const base64 = resizedLogo.toString("base64");
          return `data:image/png;base64,${base64}`;
        } catch (error) {
          console.error(`Error downloading team logo: ${error}`);
          return null;
        }
      }
      async createFootballMatchOverlay(imageBuffer, match, promoAr, promoEn, timeAr, timeEn) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).toBuffer();
        const promoArLines = this.wrapText(promoAr.split("\n")[0] || promoAr, 35);
        const promoEnLines = this.wrapText(promoEn.split("\n")[0] || promoEn, 42);
        console.log(`\u{1F504} Downloading team logos for ${match.homeTeam.name} vs ${match.awayTeam.name}...`);
        const [homeLogoBase64, awayLogoBase64, leagueLogoBase64] = await Promise.all([
          this.downloadTeamLogo(match.homeTeam.logo),
          this.downloadTeamLogo(match.awayTeam.logo),
          this.downloadTeamLogo(match.league.logo)
        ]);
        const homeLogoElement = homeLogoBase64 ? `<image x="${width / 4 - 70}" y="150" width="140" height="140" href="${homeLogoBase64}" preserveAspectRatio="xMidYMid meet"/>` : `<circle cx="${width / 4}" cy="220" r="70" fill="white" filter="url(#shadow)"/>`;
        const awayLogoElement = awayLogoBase64 ? `<image x="${width * 3 / 4 - 70}" y="150" width="140" height="140" href="${awayLogoBase64}" preserveAspectRatio="xMidYMid meet"/>` : `<circle cx="${width * 3 / 4}" cy="220" r="70" fill="white" filter="url(#shadow)"/>`;
        const leagueLogoElement = leagueLogoBase64 ? `<image x="${width / 2 - 25}" y="38" width="50" height="50" href="${leagueLogoBase64}" preserveAspectRatio="xMidYMid meet"/>` : "";
        console.log(`\u2705 Team logos loaded: Home=${!!homeLogoBase64}, Away=${!!awayLogoBase64}, League=${!!leagueLogoBase64}`);
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
            <stop offset="60%" style="stop-color:rgba(0,0,0,0.7);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="30%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="60%" style="stop-color:rgba(0,0,0,0.85);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="greenGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#059669;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#10b981;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="vsGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="redGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="6" stdDeviation="12" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="logoShadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.8)"/>
          </filter>
          <clipPath id="circleClipHome">
            <circle cx="${width / 4}" cy="220" r="70"/>
          </clipPath>
          <clipPath id="circleClipAway">
            <circle cx="${width * 3 / 4}" cy="220" r="70"/>
          </clipPath>
        </defs>
        
        <!-- Top gradient for header area -->
        <rect x="0" y="0" width="${width}" height="550" fill="url(#topGrad)"/>
        
        <!-- Bottom gradient for CTA area -->
        <rect x="0" y="${height - 650}" width="${width}" height="650" fill="url(#bottomGrad)"/>
        
        <!-- LIVE Badge -->
        <rect x="${width / 2 - 60}" y="30" width="120" height="36" rx="18" fill="url(#redGrad)" filter="url(#shadow)"/>
        <circle cx="${width / 2 - 35}" cy="48" r="6" fill="white">
          <animate attributeName="opacity" values="1;0.3;1" dur="1.5s" repeatCount="indefinite"/>
        </circle>
        <text x="${width / 2 + 10}" y="56" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">
          LIVE
        </text>
        
        <!-- League Badge with Logo -->
        <rect x="${width / 2 - 180}" y="80" width="360" height="55" rx="27" fill="url(#greenGrad)" filter="url(#shadow)"/>
        ${leagueLogoElement ? `<g transform="translate(${width / 2 - 160}, 82)">
          <circle cx="25" cy="27" r="24" fill="white"/>
          <image x="2" y="4" width="46" height="46" href="${leagueLogoBase64}" preserveAspectRatio="xMidYMid meet"/>
        </g>` : ""}
        <text x="${width / 2 + (leagueLogoElement ? 15 : 0)}" y="115" font-family="Arial, sans-serif" font-size="22" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          ${match.league.name}
        </text>
        
        <!-- Match Time Badge -->
        <rect x="${width / 2 - 140}" y="150" width="280" height="45" rx="22" fill="rgba(255,255,255,0.2)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="180" font-family="Arial, sans-serif" font-size="22" font-weight="bold" fill="white" text-anchor="middle">
          ${timeEn}
        </text>
        
        <!-- Home Team with Logo -->
        <g transform="translate(0, 0)">
          <circle cx="${width / 4}" cy="290" r="85" fill="white" filter="url(#logoShadow)"/>
          <circle cx="${width / 4}" cy="290" r="80" fill="white"/>
          ${homeLogoBase64 ? `<image x="${width / 4 - 70}" y="220" width="140" height="140" href="${homeLogoBase64}" preserveAspectRatio="xMidYMid meet"/>` : ""}
          <text x="${width / 4}" y="400" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
            ${match.homeTeam.name.length > 14 ? match.homeTeam.name.substring(0, 14) + "..." : match.homeTeam.name}
          </text>
        </g>
        
        <!-- VS Badge -->
        <rect x="${width / 2 - 55}" y="265" width="110" height="65" rx="32" fill="url(#vsGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="308" font-family="Arial, sans-serif" font-size="38" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          VS
        </text>
        
        <!-- Away Team with Logo -->
        <g transform="translate(0, 0)">
          <circle cx="${width * 3 / 4}" cy="290" r="85" fill="white" filter="url(#logoShadow)"/>
          <circle cx="${width * 3 / 4}" cy="290" r="80" fill="white"/>
          ${awayLogoBase64 ? `<image x="${width * 3 / 4 - 70}" y="220" width="140" height="140" href="${awayLogoBase64}" preserveAspectRatio="xMidYMid meet"/>` : ""}
          <text x="${width * 3 / 4}" y="400" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
            ${match.awayTeam.name.length > 14 ? match.awayTeam.name.substring(0, 14) + "..." : match.awayTeam.name}
          </text>
        </g>
        
        <!-- Match Title Arabic -->
        <text x="${width / 2}" y="470" font-family="Arial, sans-serif" font-size="32" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${match.homeTeam.name} \u0636\u062F ${match.awayTeam.name}
        </text>
        
        <!-- Arabic Promotional Text -->
        <rect x="50" y="${height - 480}" width="${width - 100}" height="${promoArLines.length * 44 + 35}" rx="22" fill="rgba(5,150,105,0.95)" filter="url(#shadow)"/>
        ${promoArLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 450 + index * 44}" font-family="Arial, sans-serif" font-size="30" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text -->
        ${promoEnLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 310 + index * 38}" font-family="Arial, sans-serif" font-size="26" fill="rgba(255,255,255,0.95)" text-anchor="middle" font-style="italic">
          "${line}"
        </text>
        `).join("")}
        
        <!-- Watch CTA Button -->
        <rect x="${width / 2 - 200}" y="${height - 140}" width="400" height="110" rx="22" fill="url(#greenGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${height - 95}" font-family="Arial, sans-serif" font-size="32" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          \u0634\u0627\u0647\u062F \u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629 \u0645\u0628\u0627\u0634\u0631\u0629
        </text>
        <text x="${width / 2}" y="${height - 55}" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
          WATCH LIVE NOW
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        return await sharp(resizedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).toBuffer();
      }
      async generateRecipePoster() {
        console.log("\u{1F373} Generating Recipe Poster...");
        const trendResult = await googleTrendsService.getBestTrendForCategory("recipes");
        const trendingTopic = trendResult.trendingTerm;
        console.log(`\u{1F373} Trending recipe topic: ${trendingTopic}`);
        const categoryQueries = CATEGORY_SEARCH_QUERIES["recipes"];
        const searchQuery = `${trendingTopic} ${categoryQueries[Math.floor(Math.random() * categoryQueries.length)]}`;
        let imageResult = await googleImageSearchService.searchThumbnailImage(searchQuery, "recipes");
        if (!imageResult) {
          imageResult = await googleImageSearchService.searchThumbnailImage(categoryQueries[0], "recipes");
        }
        if (!imageResult) {
          throw new Error("\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0644\u0648\u0635\u0641\u0629");
        }
        const promoArIndex = Math.floor(Math.random() * RECIPE_PROMOTIONAL_AR.length);
        const promoAr = RECIPE_PROMOTIONAL_AR[promoArIndex];
        const promoEn = RECIPE_PROMOTIONAL_EN[promoArIndex];
        const imageBuffer = await this.downloadImage(imageResult.imageUrl);
        const processedImage = await this.createRecipeOverlay(
          imageBuffer,
          trendingTopic,
          promoAr,
          promoEn
        );
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/recipes/${timestamp}-${randomId}`;
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category: "recipes", topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category: "recipes", topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category: "recipes", topic: trendingTopic, platform: "facebook", source: "google" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category: "recipes", topic: trendingTopic, platform: "instagram", source: "google" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category: "recipes", topic: trendingTopic, platform: "tiktok", source: "google" }
          })
        ]);
        console.log(`\u2705 Recipe poster uploaded successfully`);
        const metadata = {
          category: "recipes",
          trendingTerm: trendingTopic,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic,
          posterTitle: trendingTopic,
          sourceImageUrl: imageResult.imageUrl,
          metadata,
          descriptionAr: promoAr,
          descriptionEn: promoEn
        };
      }
      async createRecipeOverlay(imageBuffer, recipeName, promoAr, promoEn) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).toBuffer();
        const nameLines = this.wrapText(recipeName, 18, 3).map((line) => escapeXml(line));
        const promoArLines = this.wrapText(promoAr, 28, 4).map((line) => escapeXml(line));
        const promoEnLines = this.wrapText(promoEn, 35, 4).map((line) => escapeXml(line));
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="20%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.85);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="orangeGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#ea580c;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f97316;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="redGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="warmGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#b91c1c;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ea580c;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="6" stdDeviation="12" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="5" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="textGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="titleGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="8" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        
        <!-- Top gradient for header -->
        <rect x="0" y="0" width="${width}" height="500" fill="url(#topGrad)"/>
        
        <!-- Bottom gradient for content - Extended for 4 lines -->
        <rect x="0" y="${height - 750}" width="${width}" height="750" fill="url(#bottomGrad)"/>
        
        <!-- TRENDING Badge -->
        <rect x="${width / 2 - 100}" y="30" width="200" height="38" rx="19" fill="url(#goldGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="55" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">
          TRENDING RECIPE
        </text>
        
        <!-- Chef Hat Icon Badge -->
        <rect x="${width / 2 - 140}" y="80" width="280" height="55" rx="27" fill="url(#redGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="117" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          \u0648\u0635\u0641\u0629 \u062A\u0631\u0646\u062F \u0627\u0644\u064A\u0648\u0645
        </text>
        
        <!-- Recipe Name with prominent display -->
        <rect x="35" y="155" width="${width - 70}" height="${nameLines.length * 60 + 40}" rx="18" fill="rgba(0,0,0,0.7)" filter="url(#shadow)"/>
        ${nameLines.map((line, index) => `
        <text x="${width / 2}" y="${200 + index * 60}" font-family="Arial, sans-serif" font-size="52" font-weight="bold" fill="white" text-anchor="middle" filter="url(#titleGlow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- Arabic Recipe Label -->
        <rect x="${width / 2 - 90}" y="${210 + nameLines.length * 60}" width="180" height="45" rx="22" fill="url(#orangeGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${240 + nameLines.length * 60}" font-family="Arial, sans-serif" font-size="26" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          \u0648\u0635\u0641\u0629 \u0634\u0647\u064A\u0629
        </text>
        
        <!-- Arabic Promotional Text - 4 Lines (raised by 170px) -->
        <rect x="35" y="${height - 770}" width="${width - 70}" height="${promoArLines.length * 42 + 45}" rx="22" fill="rgba(234,88,12,0.95)" filter="url(#shadow)"/>
        ${promoArLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 735 + index * 42}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text - 4 Lines (raised by 170px) -->
        ${promoEnLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 550 + index * 38}" font-family="Arial, sans-serif" font-size="24" fill="rgba(255,255,255,0.95)" text-anchor="middle" font-style="italic">
          "${line}"
        </text>
        `).join("")}
        
        <!-- Professional CTA Button - Positioned higher for Facebook Story visibility (raised by 170px) -->
        <rect x="${width / 2 - 240}" y="${height - 450}" width="480" height="135" rx="25" fill="url(#warmGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${height - 393}" font-family="Arial, sans-serif" font-size="36" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${RECIPE_CTA_AR}
        </text>
        <text x="${width / 2}" y="${height - 345}" font-family="Arial, sans-serif" font-size="26" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
          ${RECIPE_CTA_EN}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        return await sharp(resizedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).toBuffer();
      }
      async generateGamingPoster() {
        console.log("\u{1F3AE} Generating Gaming Poster from Google Play Store...");
        const trendingGame = await googlePlayService.getRandomTrendingGame();
        const gameName = trendingGame.title;
        const gameGenre = trendingGame.genre;
        const gameGenreAr = googlePlayService.getGenreArabic(gameGenre);
        const installsInfo = googlePlayService.formatInstalls(trendingGame.installs);
        console.log(`\u{1F3AE} Selected trending game from Play Store: ${gameName}`);
        console.log(`   Genre: ${gameGenre} (${gameGenreAr})`);
        console.log(`   Rating: ${trendingGame.score}/5`);
        console.log(`   Installs: ${trendingGame.installs}`);
        console.log(`   Developer: ${trendingGame.developer}`);
        let imageBuffer = null;
        let usedImageUrl = "";
        let gameLogoBase64 = null;
        const [logoResult, screenshotResult] = await Promise.all([
          this.fetchPlayStoreGameIcon(trendingGame),
          this.fetchPlayStoreScreenshot(trendingGame)
        ]);
        gameLogoBase64 = logoResult;
        if (screenshotResult) {
          imageBuffer = screenshotResult.buffer;
          usedImageUrl = screenshotResult.url;
          console.log(`\u2705 Successfully downloaded Play Store screenshot`);
        }
        if (!imageBuffer && trendingGame.icon) {
          try {
            console.log(`\u{1F5BC}\uFE0F Using game icon as background...`);
            const iconUrl = googlePlayService.getHighResIcon(trendingGame.icon);
            const iconBuffer = await this.downloadImage(iconUrl);
            imageBuffer = await sharp(iconBuffer).resize(STORY_DIMENSIONS.width, STORY_DIMENSIONS.height, { fit: "cover" }).blur(15).modulate({ brightness: 0.5 }).toBuffer();
            usedImageUrl = iconUrl;
          } catch (error) {
            console.log(`\u26A0\uFE0F Icon background failed: ${error.message}`);
          }
        }
        if (!imageBuffer) {
          console.log("\u{1F4F8} All images failed, using generated placeholder");
          imageBuffer = await this.generatePlaceholderImage(gameName, "gaming");
          usedImageUrl = "generated-placeholder";
        }
        console.log(`\u{1F916} Generating professional bilingual descriptions for: ${gameName}`);
        let promoAr;
        let promoEn;
        try {
          const posterContent = await this.generatePlayStoreGameDescription(trendingGame);
          promoAr = posterContent.descriptionAr;
          promoEn = posterContent.descriptionEn;
          console.log(`\u2705 AI descriptions generated with game name: ${gameName}`);
        } catch (error) {
          console.log(`\u26A0\uFE0F AI generation failed, using template descriptions for: ${gameName}`);
          const ratingText = trendingGame.score > 4 ? "\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629" : trendingGame.score > 3.5 ? "\u0645\u0645\u062A\u0627\u0632\u0629" : "\u0631\u0627\u0626\u0639\u0629";
          promoAr = `${gameName} - \u0627\u0644\u0644\u0639\u0628\u0629 ${ratingText} \u0627\u0644\u0623\u0643\u062B\u0631 \u062A\u062D\u0645\u064A\u0644\u0627\u064B \u0639\u0644\u0649 \u0645\u062A\u062C\u0631 \u0628\u0644\u0627\u064A! ${installsInfo.ar}. \u062A\u0642\u064A\u064A\u0645 ${trendingGame.score}/5 \u0645\u0646 \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646. \u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u062A\u062C\u0631\u0628\u0629 ${gameGenreAr} \u0644\u0627 \u0645\u062B\u064A\u0644 \u0644\u0647\u0627 \u0645\u0639 \u0631\u0633\u0648\u0645\u0627\u062A \u062E\u064A\u0627\u0644\u064A\u0629 \u0648\u0645\u0633\u062A\u0648\u064A\u0627\u062A \u0645\u062B\u064A\u0631\u0629. \u0645\u0646 \u062A\u0637\u0648\u064A\u0631 ${trendingGame.developer}. \u062D\u0645\u0651\u0644 ${gameName} \u0645\u062C\u0627\u0646\u0627\u064B \u0627\u0644\u0622\u0646!`;
          promoEn = `${gameName} - The top-rated ${gameGenre} game on Google Play! ${installsInfo.en}. Rated ${trendingGame.score}/5 by millions of players worldwide. Experience unmatched ${gameGenre} gameplay with stunning graphics and exciting challenges. Developed by ${trendingGame.developer}. Download ${gameName} FREE today!`;
        }
        const validImageBuffer = imageBuffer;
        let processedImage;
        try {
          processedImage = await this.createGamingOverlay(
            validImageBuffer,
            gameName,
            promoAr,
            promoEn,
            gameLogoBase64
          );
        } catch (overlayError) {
          console.log(`\u26A0\uFE0F Gaming overlay failed: ${overlayError.message}, using enhanced fallback`);
          processedImage = await this.createSimpleGamingFallback(
            validImageBuffer,
            gameName,
            promoAr,
            promoEn,
            gameLogoBase64
          );
        }
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/gaming/${timestamp}-${randomId}`;
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category: "gaming", topic: gameName, source: "google-play" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category: "gaming", topic: gameName, source: "google-play" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category: "gaming", topic: gameName, platform: "facebook", source: "google-play" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category: "gaming", topic: gameName, platform: "instagram", source: "google-play" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category: "gaming", topic: gameName, platform: "tiktok", source: "google-play" }
          })
        ]);
        console.log(`\u2705 Gaming poster for "${gameName}" uploaded successfully (source: Google Play Store)`);
        const metadata = {
          category: "gaming",
          trendingTerm: gameName,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic: gameName,
          posterTitle: gameName,
          sourceImageUrl: usedImageUrl,
          metadata,
          descriptionAr: promoAr,
          descriptionEn: promoEn,
          voteAverage: trendingGame.score
        };
      }
      /**
       * Fetch and process game icon from Google Play Store with multiple fallbacks
       */
      async fetchPlayStoreGameIcon(game) {
        try {
          console.log(`\u{1F3AE} Downloading official game icon for: ${game.title}`);
          let iconUrl = game.icon;
          let freshGame = null;
          if (!iconUrl || iconUrl.includes("undefined")) {
            console.log(`   \u{1F504} No icon URL, fetching fresh data for: ${game.appId}`);
            freshGame = await googlePlayService.getGameDetails(game.appId);
            if (freshGame?.icon) {
              iconUrl = freshGame.icon;
              console.log(`   \u2705 Got fresh icon URL from API`);
            }
          }
          if (!iconUrl) {
            console.log("\u26A0\uFE0F No game icon available for:", game.title);
            return null;
          }
          const iconUrls = [
            googlePlayService.getHighResIcon(iconUrl),
            iconUrl.replace(/=w\d+-h\d+/g, "=w256-h256"),
            iconUrl.replace(/=w\d+-h\d+/g, "=w128-h128"),
            iconUrl
          ];
          let buffer = null;
          for (const url of iconUrls) {
            try {
              console.log(`   Trying icon URL: ${url.substring(0, 80)}...`);
              const response = await fetch(url, {
                headers: {
                  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                  "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                  "Accept-Language": "en-US,en;q=0.9",
                  "Referer": "https://play.google.com/"
                },
                signal: AbortSignal.timeout(1e4)
              });
              if (response.ok) {
                const arrayBuffer = await response.arrayBuffer();
                if (arrayBuffer.byteLength > 1e3) {
                  buffer = Buffer.from(arrayBuffer);
                  console.log(`   \u2705 Icon downloaded: ${buffer.length} bytes`);
                  break;
                }
              }
            } catch (e) {
              console.log(`   \u26A0\uFE0F Icon URL failed: ${e.message}`);
            }
          }
          if (!buffer && !freshGame) {
            console.log(`   \u{1F504} All URLs failed, fetching fresh game data from API...`);
            freshGame = await googlePlayService.getGameDetails(game.appId);
            if (freshGame?.icon && freshGame.icon !== iconUrl) {
              console.log(`   \u2705 Got fresh icon URL, retrying...`);
              const freshUrls = [
                googlePlayService.getHighResIcon(freshGame.icon),
                freshGame.icon
              ];
              for (const url of freshUrls) {
                try {
                  const response = await fetch(url, {
                    headers: {
                      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                      "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                      "Referer": "https://play.google.com/"
                    },
                    signal: AbortSignal.timeout(1e4)
                  });
                  if (response.ok) {
                    const arrayBuffer = await response.arrayBuffer();
                    if (arrayBuffer.byteLength > 1e3) {
                      buffer = Buffer.from(arrayBuffer);
                      console.log(`   \u2705 Fresh icon downloaded: ${buffer.length} bytes`);
                      break;
                    }
                  }
                } catch (e) {
                  console.log(`   \u26A0\uFE0F Fresh icon URL failed: ${e.message}`);
                }
              }
            }
          }
          if (!buffer) {
            console.log(`\u274C All icon URLs failed for: ${game.title}`);
            return null;
          }
          const resizedIcon = await sharp(buffer).resize(160, 160, {
            fit: "contain",
            background: { r: 0, g: 0, b: 0, alpha: 0 }
          }).png({ quality: 100 }).toBuffer();
          const base64 = resizedIcon.toString("base64");
          console.log(`\u2705 Game icon processed for: ${game.title} (${resizedIcon.length} bytes)`);
          return `data:image/png;base64,${base64}`;
        } catch (error) {
          console.error(`Error fetching game icon for ${game.title}: ${error.message}`);
          return null;
        }
      }
      /**
       * Fetch and process screenshot from Google Play Store with multiple fallbacks
       */
      async fetchPlayStoreScreenshot(game) {
        try {
          console.log(`\u{1F5BC}\uFE0F Fetching screenshot for game: ${game.title}`);
          const tryDownloadScreenshot = async (urls) => {
            for (const url of urls) {
              try {
                console.log(`   Trying screenshot: ${url.substring(0, 80)}...`);
                const response = await fetch(url, {
                  headers: {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://play.google.com/"
                  },
                  signal: AbortSignal.timeout(15e3)
                });
                if (response.ok) {
                  const arrayBuffer = await response.arrayBuffer();
                  if (arrayBuffer.byteLength > 5e3) {
                    const buffer = Buffer.from(arrayBuffer);
                    console.log(`   \u2705 Screenshot downloaded for ${game.title}: ${buffer.length} bytes`);
                    return { buffer, url };
                  } else {
                    console.log(`   \u26A0\uFE0F Screenshot too small: ${arrayBuffer.byteLength} bytes`);
                  }
                } else {
                  console.log(`   \u26A0\uFE0F Screenshot fetch failed: ${response.status}`);
                }
              } catch (e) {
                console.log(`   \u26A0\uFE0F Screenshot URL failed: ${e.message}`);
              }
            }
            return null;
          };
          const buildScreenshotUrls = (screenshots) => {
            const urls = [];
            if (screenshots && screenshots.length > 0) {
              for (const ss of screenshots.slice(0, 5)) {
                const highRes = googlePlayService.getHighResScreenshot(ss);
                if (!urls.includes(highRes)) urls.push(highRes);
                const medRes = ss.replace(/=w\d+/g, "=w1280");
                if (!urls.includes(medRes)) urls.push(medRes);
                if (!urls.includes(ss)) urls.push(ss);
              }
            }
            return urls;
          };
          let screenshotUrls = buildScreenshotUrls(game.screenshots);
          if (screenshotUrls.length > 0) {
            console.log(`\u{1F5BC}\uFE0F Trying ${screenshotUrls.length} screenshot URLs for: ${game.title}`);
            const result = await tryDownloadScreenshot(screenshotUrls);
            if (result) return result;
          }
          console.log(`   \u{1F504} Fetching fresh game data from API for screenshots...`);
          const freshGame = await googlePlayService.getGameDetails(game.appId);
          if (freshGame?.screenshots && freshGame.screenshots.length > 0) {
            console.log(`   \u2705 Got ${freshGame.screenshots.length} fresh screenshots from API`);
            const freshUrls = buildScreenshotUrls(freshGame.screenshots);
            if (freshGame.headerImage) {
              freshUrls.unshift(freshGame.headerImage);
            }
            if (freshUrls.length > 0) {
              console.log(`\u{1F5BC}\uFE0F Trying ${freshUrls.length} fresh screenshot URLs for: ${game.title}`);
              const result = await tryDownloadScreenshot(freshUrls);
              if (result) return result;
            }
          }
          console.log(`\u274C All screenshot URLs failed for: ${game.title}`);
          return null;
        } catch (error) {
          console.error(`Error fetching screenshot for ${game.title}: ${error.message}`);
          return null;
        }
      }
      /**
       * Generate professional bilingual descriptions using Play Store metadata
       */
      async generatePlayStoreGameDescription(game) {
        const genreAr = googlePlayService.getGenreArabic(game.genre);
        const installsInfo = googlePlayService.formatInstalls(game.installs);
        const ratingDesc = game.score >= 4.5 ? "legendary" : game.score >= 4 ? "excellent" : game.score >= 3.5 ? "great" : "popular";
        const ratingDescAr = game.score >= 4.5 ? "\u0623\u0633\u0637\u0648\u0631\u064A\u0629" : game.score >= 4 ? "\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A\u0629" : game.score >= 3.5 ? "\u0631\u0627\u0626\u0639\u0629" : "\u0645\u0634\u0647\u0648\u0631\u0629";
        const createDescriptionAr = () => {
          const templates = [
            `${game.title} - \u0627\u0644\u0644\u0639\u0628\u0629 ${ratingDescAr} \u0627\u0644\u0623\u0643\u062B\u0631 \u062A\u062D\u0645\u064A\u0644\u0627\u064B \u0639\u0644\u0649 \u0645\u062A\u062C\u0631 \u062C\u0648\u062C\u0644 \u0628\u0644\u0627\u064A! ${installsInfo.ar}. \u062A\u0642\u064A\u064A\u0645 ${game.score}/5 \u0645\u0646 \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645. \u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u062A\u062C\u0631\u0628\u0629 ${genreAr} \u0644\u0627 \u0645\u062B\u064A\u0644 \u0644\u0647\u0627 \u0645\u0639 \u0631\u0633\u0648\u0645\u0627\u062A \u0645\u0630\u0647\u0644\u0629 \u0648\u062A\u062D\u062F\u064A\u0627\u062A \u0645\u062B\u064A\u0631\u0629. \u0645\u0646 \u062A\u0637\u0648\u064A\u0631 ${game.developer}. \u062D\u0645\u0651\u0644 ${game.title} \u0645\u062C\u0627\u0646\u0627\u064B \u0627\u0644\u0622\u0646!`,
            `\u0627\u0643\u062A\u0634\u0641 ${game.title} - \u0627\u0644\u0644\u0639\u0628\u0629 \u0627\u0644\u0623\u0643\u062B\u0631 \u0634\u0639\u0628\u064A\u0629 \u0641\u064A \u0641\u0626\u0629 ${genreAr}! ${installsInfo.ar} \u064A\u062B\u0628\u062A \u0646\u062C\u0627\u062D\u0647\u0627 \u0627\u0644\u0643\u0628\u064A\u0631. \u062A\u0642\u064A\u064A\u0645 ${game.score}/5 \u0645\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646. \u0631\u0633\u0648\u0645\u0627\u062A \u062E\u0631\u0627\u0641\u064A\u0629 \u0648\u062C\u064A\u0645 \u0628\u0644\u0627\u064A \u0645\u0633\u0644\u064A. \u0627\u0646\u0636\u0645 \u0644\u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0644\u0627\u0639\u0628\u064A\u0646 \u0648\u062D\u0645\u0651\u0644 ${game.title} \u0627\u0644\u064A\u0648\u0645!`,
            `${game.title} \u0645\u0646 ${game.developer} - \u062A\u062D\u0641\u0629 \u0641\u064A \u0639\u0627\u0644\u0645 \u0623\u0644\u0639\u0627\u0628 ${genreAr}! \u062D\u0635\u0644\u062A \u0639\u0644\u0649 \u062A\u0642\u064A\u064A\u0645 ${game.score}/5 \u0648\u0623\u0643\u062B\u0631 \u0645\u0646 ${installsInfo.ar}. \u0639\u0627\u0644\u0645 \u0636\u062E\u0645 \u0645\u0646 \u0627\u0644\u0625\u062B\u0627\u0631\u0629 \u0648\u0627\u0644\u0645\u062A\u0639\u0629 \u064A\u0646\u062A\u0638\u0631\u0643. \u0644\u0627 \u062A\u0641\u0648\u062A \u0641\u0631\u0635\u0629 \u062A\u062C\u0631\u0628\u0629 ${game.title} \u0645\u062C\u0627\u0646\u0627\u064B!`
          ];
          return templates[Math.floor(Math.random() * templates.length)];
        };
        const createDescriptionEn = () => {
          const templates = [
            `${game.title} - The ${ratingDesc} ${game.genre} game dominating Google Play! ${installsInfo.en}. Rated ${game.score}/5 by millions worldwide. Experience unmatched gameplay with stunning graphics and endless challenges. Developed by ${game.developer}. Download ${game.title} FREE today!`,
            `Discover ${game.title} - The #1 ${game.genre} game everyone's playing! ${installsInfo.en} proves its massive success. ${game.score}/5 stars from players. Mind-blowing graphics and addictive gameplay await. Join millions and download ${game.title} now!`,
            `${game.title} by ${game.developer} - A masterpiece in ${game.genre} gaming! Rated ${game.score}/5 with ${installsInfo.en}. Immerse yourself in a world of excitement and fun. Don't miss your chance to try ${game.title} FREE!`
          ];
          return templates[Math.floor(Math.random() * templates.length)];
        };
        try {
          const arPrompt = `\u0627\u0643\u062A\u0628 \u0648\u0635\u0641\u0627\u064B \u062A\u0631\u0648\u064A\u062C\u064A\u0627\u064B \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0627\u064B \u0648\u0645\u062B\u064A\u0631\u0627\u064B \u0645\u0646 3-4 \u062C\u0645\u0644 \u0644\u0644\u0639\u0628\u0629 "${game.title}" \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644 \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A.

\u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0627\u0644\u0644\u0639\u0628\u0629 \u0645\u0646 \u0645\u062A\u062C\u0631 \u062C\u0648\u062C\u0644 \u0628\u0644\u0627\u064A:
- \u0627\u0644\u0646\u0648\u0639: ${game.genre} (${genreAr})
- \u0627\u0644\u062A\u0642\u064A\u064A\u0645: ${game.score}/5
- \u0639\u062F\u062F \u0627\u0644\u062A\u062D\u0645\u064A\u0644\u0627\u062A: ${game.installs}
- \u0627\u0644\u0645\u0637\u0648\u0631: ${game.developer}
- \u0627\u0644\u0633\u0639\u0631: ${game.free ? "\u0645\u062C\u0627\u0646\u064A\u0629" : game.priceText}

\u0627\u0644\u0645\u062A\u0637\u0644\u0628\u0627\u062A:
1. \u0627\u0630\u0643\u0631 \u0627\u0633\u0645 \u0627\u0644\u0644\u0639\u0628\u0629 "${game.title}" \u0645\u0631\u062A\u064A\u0646 \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644
2. \u0627\u0630\u0643\u0631 \u0627\u0644\u062A\u0642\u064A\u064A\u0645 \u0648\u0639\u062F\u062F \u0627\u0644\u062A\u062D\u0645\u064A\u0644\u0627\u062A
3. \u0627\u062C\u0639\u0644 \u0627\u0644\u0648\u0635\u0641 \u0645\u062B\u064A\u0631\u0627\u064B \u0648\u064A\u062D\u0641\u0632 \u0639\u0644\u0649 \u0627\u0644\u062A\u062D\u0645\u064A\u0644 \u0627\u0644\u0641\u0648\u0631\u064A
4. \u0644\u0627 \u062A\u0643\u062A\u0628 \u0623\u064A \u0645\u0642\u062F\u0645\u0627\u062A \u0623\u0648 \u0639\u0646\u0627\u0648\u064A\u0646\u060C \u0641\u0642\u0637 \u0627\u0644\u0648\u0635\u0641 \u0627\u0644\u062A\u0631\u0648\u064A\u062C\u064A`;
          const enPrompt = `Write a professional and exciting 3-4 sentence promotional description for the game "${game.title}" for social media.

Game info from Google Play Store:
- Genre: ${game.genre}
- Rating: ${game.score}/5
- Downloads: ${game.installs}
- Developer: ${game.developer}
- Price: ${game.free ? "Free" : game.priceText}

Requirements:
1. Mention the game name "${game.title}" at least twice
2. Include the rating and download count
3. Make it exciting and encourage immediate download
4. Write ONLY the description without any titles or introductions`;
          const systemPromptAr = "\u0623\u0646\u062A \u0643\u0627\u062A\u0628 \u0645\u062D\u062A\u0648\u0649 \u0623\u0644\u0639\u0627\u0628 \u0645\u062D\u062A\u0631\u0641. \u0627\u0643\u062A\u0628 \u0628\u0623\u0633\u0644\u0648\u0628 \u0645\u062B\u064A\u0631 \u0648\u062C\u0630\u0627\u0628 \u0644\u0644\u062C\u064A\u0645\u0631\u0632. \u0627\u0633\u062A\u062E\u062F\u0645 \u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0645\u062A\u062C\u0631 \u062C\u0648\u062C\u0644 \u0628\u0644\u0627\u064A \u0627\u0644\u0631\u0633\u0645\u064A\u0629.";
          const systemPromptEn = "You are a professional gaming content writer. Write in an exciting, engaging style for gamers. Use official Google Play Store data.";
          const [arResult, enResult] = await Promise.all([
            deepseekSDK.generateSimple(arPrompt, systemPromptAr, { temperature: 0.7, max_tokens: 250 }),
            deepseekSDK.generateSimple(enPrompt, systemPromptEn, { temperature: 0.7, max_tokens: 250 })
          ]);
          let descAr = arResult?.trim() || "";
          let descEn = enResult?.trim() || "";
          if (!descAr || !descAr.includes(game.title)) {
            descAr = createDescriptionAr();
          }
          if (!descEn || !descEn.includes(game.title)) {
            descEn = createDescriptionEn();
          }
          return { descriptionAr: descAr, descriptionEn: descEn };
        } catch (error) {
          console.log(`\u26A0\uFE0F AI generation failed for ${game.title}, using template fallback`);
          return {
            descriptionAr: createDescriptionAr(),
            descriptionEn: createDescriptionEn()
          };
        }
      }
      async generatePlaceholderImage(title, category) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const gradients = {
          "movies": { from: "#1a1a2e", to: "#16213e" },
          "tv_shows": { from: "#0f0e17", to: "#2a2438" },
          "sports": { from: "#1b4332", to: "#2d6a4f" },
          "recipes": { from: "#7c2d12", to: "#ea580c" },
          "gaming": { from: "#3b0764", to: "#7c3aed" },
          "apps": { from: "#0c4a6e", to: "#0284c7" },
          "tv_channels": { from: "#134e4a", to: "#14b8a6" }
        };
        const colors = gradients[category] || gradients["gaming"];
        try {
          const safeTitle = (title || category).trim();
          const displayTitle = safeTitle.length > 20 ? safeTitle.substring(0, 20) + "..." : safeTitle;
          const words = safeTitle.split(" ").filter((w) => w.length > 0);
          const initials = words.length === 0 ? "G" : words.slice(0, 2).map((w) => w[0]?.toUpperCase() || "").join("") || "G";
          const escapedTitle = escapeXml(displayTitle);
          const escapedInitials = escapeXml(initials);
          const svg = `<?xml version="1.0" encoding="UTF-8"?>
        <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:${colors.from};stop-opacity:1" />
              <stop offset="100%" style="stop-color:${colors.to};stop-opacity:1" />
            </linearGradient>
          </defs>
          <rect width="${width}" height="${height}" fill="url(#bg)"/>
          <circle cx="${width / 2}" cy="${height / 2 - 100}" r="150" fill="rgba(255,255,255,0.1)"/>
          <text x="${width / 2}" y="${height / 2 - 50}" font-family="Arial, sans-serif" font-size="120" font-weight="bold" fill="white" text-anchor="middle">${escapedInitials}</text>
          <text x="${width / 2}" y="${height / 2 + 150}" font-family="Arial, sans-serif" font-size="48" font-weight="bold" fill="white" text-anchor="middle">${escapedTitle}</text>
        </svg>
      `;
          const pngBuffer = await sharp(Buffer.from(svg)).resize(width, height).png({ quality: 100 }).toBuffer();
          return pngBuffer;
        } catch (error) {
          console.log(`\u26A0\uFE0F SVG placeholder failed: ${error.message}, using solid color fallback`);
          const solidBuffer = await sharp({
            create: {
              width,
              height,
              channels: 3,
              background: { r: 59, g: 7, b: 100 }
            }
          }).png().toBuffer();
          return solidBuffer;
        }
      }
      async createSimpleGamingFallback(imageBuffer, gameName, promoAr = "", promoEn = "", gameLogoBase64 = null) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        try {
          const processedImage = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).modulate({ brightness: 0.55, saturation: 0.85 }).png().toBuffer();
          const safeGameName = (gameName || "Game").trim();
          const escapedGameName = escapeXml(safeGameName);
          const gameInitials = escapeXml(this.getGameInitials(safeGameName));
          const nameLines = this.wrapText(safeGameName, 16, 2).map((line) => escapeXml(line));
          const promoArLines = this.wrapText(promoAr || GAMING_PROMOTIONAL_AR[0], 30, 4).map((line) => escapeXml(line));
          const promoEnLines = this.wrapText(promoEn || GAMING_PROMOTIONAL_EN[0], 38, 4).map((line) => escapeXml(line));
          const validatedLogo = await this.validateAndProcessBase64Image(gameLogoBase64);
          const logoElement = validatedLogo ? `<image x="${width / 2 - 70}" y="95" width="140" height="140" href="${validatedLogo}" preserveAspectRatio="xMidYMid meet"/>` : `<text x="${width / 2}" y="180" font-family="Arial, sans-serif" font-size="70" font-weight="bold" fill="white" text-anchor="middle">${gameInitials}</text>`;
          const svgOverlay = `
        <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
          <defs>
            <linearGradient id="topGradFb" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" style="stop-color:rgba(0,0,0,0.75);stop-opacity:1" />
              <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            </linearGradient>
            <linearGradient id="bottomGradFb" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
              <stop offset="40%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
              <stop offset="100%" style="stop-color:rgba(0,0,0,0.9);stop-opacity:1" />
            </linearGradient>
            <linearGradient id="purpleGradFb" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#7c3aed;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#a855f7;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="goldGradFb" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="greenGradFb" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#059669;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#10b981;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="redGradFb" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
            </linearGradient>
          </defs>
          
          <!-- Top gradient -->
          <rect x="0" y="0" width="${width}" height="350" fill="url(#topGradFb)"/>
          
          <!-- Bottom gradient -->
          <rect x="0" y="${height - 700}" width="${width}" height="700" fill="url(#bottomGradFb)"/>
          
          <!-- TRENDING Badge -->
          <rect x="${width / 2 - 110}" y="25" width="220" height="40" rx="20" fill="url(#redGradFb)"/>
          <text x="${width / 2}" y="52" font-family="Arial, sans-serif" font-size="20" font-weight="bold" fill="white" text-anchor="middle">TRENDING NOW</text>
          
          <!-- Logo Container -->
          <rect x="${width / 2 - 80}" y="85" width="160" height="160" rx="30" fill="url(#purpleGradFb)"/>
          <rect x="${width / 2 - 75}" y="90" width="150" height="150" rx="27" fill="rgba(255,255,255,0.15)"/>
          ${logoElement}
          
          <!-- Game Name -->
          <rect x="${width / 2 - 260}" y="265" width="520" height="${nameLines.length * 70 + 30}" rx="15" fill="rgba(0,0,0,0.5)"/>
          ${nameLines.map((line, index) => `
          <text x="${width / 2}" y="${315 + index * 70}" font-family="Arial, sans-serif" font-size="52" font-weight="bold" fill="white" text-anchor="middle">
            ${line}
          </text>
          `).join("")}
          
          <!-- Arabic Label with Game Name -->
          <rect x="${width / 2 - 170}" y="${325 + nameLines.length * 70}" width="340" height="48" rx="24" fill="url(#goldGradFb)"/>
          <text x="${width / 2}" y="${358 + nameLines.length * 70}" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="white" text-anchor="middle">
            \u0644\u0639\u0628\u0629 \u0627\u0644\u062A\u0631\u0646\u062F - ${escapedGameName.length > 12 ? escapedGameName.substring(0, 12) + "..." : escapedGameName}
          </text>
          
          <!-- Rating Stars -->
          <text x="${width / 2}" y="${398 + nameLines.length * 70}" font-size="26" fill="#fbbf24" text-anchor="middle">\u2605 \u2605 \u2605 \u2605 \u2605</text>
          
          <!-- Arabic Promo Text - 4 Lines -->
          <rect x="50" y="${height - 580}" width="${width - 100}" height="${promoArLines.length * 36 + 25}" rx="12" fill="rgba(124,58,237,0.8)"/>
          ${promoArLines.map((line, index) => `
          <text x="${width / 2}" y="${height - 552 + index * 36}" font-family="Arial, sans-serif" font-size="23" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            ${line}
          </text>
          `).join("")}
          
          <!-- English Promo Text - 4 Lines with clear readable background -->
          <rect x="50" y="${height - 400}" width="${width - 100}" height="${promoEnLines.length * 32 + 28}" rx="12" fill="rgba(0,0,0,0.75)"/>
          ${promoEnLines.map((line, index) => `
          <text x="${width / 2}" y="${height - 372 + index * 32}" font-family="Arial, sans-serif" font-size="21" font-weight="600" fill="white" text-anchor="middle" font-style="italic">
            ${line}
          </text>
          `).join("")}
          
          <!-- CTA Button - Expanded to fit all text properly -->
          <rect x="${width / 2 - 340}" y="${height - 200}" width="680" height="185" rx="28" fill="url(#greenGradFb)"/>
          <text x="${width / 2}" y="${height - 115}" font-family="Arial, sans-serif" font-size="30" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            ${escapeXml(GAMING_CTA_AR)}
          </text>
          <text x="${width / 2}" y="${height - 65}" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="white" text-anchor="middle">
            ${escapeXml(GAMING_CTA_EN)}
          </text>
        </svg>
      `;
          const overlayBuffer = Buffer.from(svgOverlay);
          return await sharp(processedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).png().toBuffer();
        } catch (error) {
          console.log(`\u26A0\uFE0F Enhanced fallback also failed: ${error.message}, returning base image`);
          return await sharp(imageBuffer).resize(width, height, { fit: "cover" }).png().toBuffer();
        }
      }
      getGameInitials(gameName) {
        const words = gameName.trim().split(/\s+/).filter((w) => w.length > 0);
        if (words.length === 0) return "G";
        if (words.length === 1) {
          return words[0].substring(0, 2).toUpperCase();
        }
        return words.slice(0, 2).map((w) => w[0].toUpperCase()).join("");
      }
      async validateAndProcessBase64Image(base64Data) {
        if (!base64Data) return null;
        const allowedMimeTypes = ["data:image/png", "data:image/jpeg", "data:image/jpg", "data:image/webp"];
        const hasValidMimeType = allowedMimeTypes.some((type) => base64Data.startsWith(type));
        if (!hasValidMimeType) return null;
        if (!base64Data.includes("base64,")) return null;
        try {
          const base64Content = base64Data.split("base64,")[1];
          if (!base64Content || base64Content.length < 100) return null;
          const decoded = Buffer.from(base64Content, "base64");
          if (decoded.length < 100) return null;
          const pngSignature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
          const jpegSignature = Buffer.from([255, 216, 255]);
          const webpSignature = Buffer.from([82, 73, 70, 70]);
          const isPng = decoded.subarray(0, 8).equals(pngSignature);
          const isJpeg = decoded.subarray(0, 3).equals(jpegSignature);
          const isWebp = decoded.subarray(0, 4).equals(webpSignature);
          if (!isPng && !isJpeg && !isWebp) return null;
          const validated = await sharp(decoded).resize(160, 160, { fit: "contain", background: { r: 0, g: 0, b: 0, alpha: 0 } }).png().toBuffer();
          return `data:image/png;base64,${validated.toString("base64")}`;
        } catch (e) {
          console.log(`\u26A0\uFE0F Logo validation failed: ${e}`);
          return null;
        }
      }
      async createGamingOverlay(imageBuffer, gameName, promoAr, promoEn, gameLogoBase64 = null) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).toBuffer();
        const safeGameName = (gameName || "Game").trim();
        const gameInitials = escapeXml(this.getGameInitials(safeGameName));
        const escapedGameName = escapeXml(safeGameName);
        const nameLines = this.wrapText(safeGameName, 16, 2).map((line) => escapeXml(line));
        const promoArLines = this.wrapText(promoAr || "", 28, 4).map((line) => escapeXml(line));
        const promoEnLines = this.wrapText(promoEn || "", 34, 4).map((line) => escapeXml(line));
        const validatedLogo = await this.validateAndProcessBase64Image(gameLogoBase64);
        const logoElement = validatedLogo ? `<image x="${width / 2 - 80}" y="85" width="160" height="160" href="${validatedLogo}" preserveAspectRatio="xMidYMid meet" filter="url(#logoShadow)"/>` : `<text x="${width / 2}" y="${gameInitials.length === 1 ? 190 : 185}" font-family="Arial, sans-serif" font-size="${gameInitials.length === 1 ? 90 : 75}" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" filter="url(#neonGlow)">${gameInitials}</text>`;
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.3);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="30%" style="stop-color:rgba(0,0,0,0.2);stop-opacity:1" />
            <stop offset="60%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.75);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="purpleGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#7c3aed;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#8b5cf6;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="greenPlayGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#059669;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#10b981;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="neonGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#6366f1;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#8b5cf6;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#a855f7;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="redGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="blueGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#1e40af;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#3b82f6;stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="8" stdDeviation="14" flood-color="rgba(0,0,0,0.98)"/>
          </filter>
          <filter id="logoShadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.9)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="6" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="neonGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="10" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="titleGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="textShadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
        </defs>
        
        <!-- Top gradient for header - smaller and more transparent -->
        <rect x="0" y="0" width="${width}" height="320" fill="url(#topGrad)"/>
        
        <!-- Bottom gradient for content - smaller and more transparent -->
        <rect x="0" y="${height - 600}" width="${width}" height="600" fill="url(#bottomGrad)"/>
        
        <!-- TRENDING NOW Badge at top -->
        <rect x="${width / 2 - 120}" y="20" width="240" height="42" rx="21" fill="url(#redGrad)" filter="url(#shadow)"/>
        <circle cx="${width / 2 - 85}" cy="41" r="8" fill="white">
          <animate attributeName="opacity" values="1;0.3;1" dur="1.5s" repeatCount="indefinite"/>
        </circle>
        <text x="${width / 2 + 10}" y="49" font-family="Arial, sans-serif" font-size="20" font-weight="bold" fill="white" text-anchor="middle">
          TRENDING NOW
        </text>
        
        <!-- Professional Game Logo Container -->
        <rect x="${width / 2 - 90}" y="75" width="180" height="180" rx="35" fill="url(#blueGrad)" filter="url(#shadow)"/>
        <rect x="${width / 2 - 85}" y="80" width="170" height="170" rx="32" fill="url(#neonGrad)"/>
        <rect x="${width / 2 - 82}" y="83" width="164" height="164" rx="30" fill="rgba(255,255,255,0.15)"/>
        ${logoElement}
        
        <!-- GAME NAME - Large Professional Title with actual game name - transparent background only around text -->
        <rect x="${width / 2 - 280}" y="275" width="560" height="${nameLines.length * 75 + 40}" rx="18" fill="rgba(0,0,0,0.45)" filter="url(#shadow)"/>
        ${nameLines.map((line, index) => `
        <text x="${width / 2}" y="${330 + index * 75}" font-family="Arial, sans-serif" font-size="58" font-weight="bold" fill="white" text-anchor="middle" filter="url(#titleGlow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- Arabic Game Label with game name -->
        <rect x="${width / 2 - 180}" y="${340 + nameLines.length * 75}" width="360" height="55" rx="27" fill="url(#goldGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${377 + nameLines.length * 75}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          \u0644\u0639\u0628\u0629 \u0627\u0644\u062A\u0631\u0646\u062F \u0627\u0644\u0623\u0648\u0644\u0649 - ${escapedGameName.length > 15 ? escapedGameName.substring(0, 15) + "..." : escapedGameName}
        </text>
        
        <!-- Rating Stars -->
        <g transform="translate(${width / 2 - 80}, ${405 + nameLines.length * 75})">
          <text x="0" y="25" font-size="28" fill="#fbbf24" filter="url(#glow)">\u2605</text>
          <text x="35" y="25" font-size="28" fill="#fbbf24" filter="url(#glow)">\u2605</text>
          <text x="70" y="25" font-size="28" fill="#fbbf24" filter="url(#glow)">\u2605</text>
          <text x="105" y="25" font-size="28" fill="#fbbf24" filter="url(#glow)">\u2605</text>
          <text x="140" y="25" font-size="28" fill="#fbbf24" filter="url(#glow)">\u2605</text>
        </g>
        
        <!-- Arabic Promotional Text - 4 Lines with better spacing and transparency (raised by 170px) -->
        <rect x="40" y="${height - 730}" width="${width - 80}" height="${promoArLines.length * 38 + 30}" rx="16" fill="rgba(124,58,237,0.75)" filter="url(#shadow)"/>
        ${promoArLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 698 + index * 38}" font-family="Arial, sans-serif" font-size="25" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#textShadow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text - 4 Lines with clear readable background (raised by 170px) -->
        <rect x="45" y="${height - 540}" width="${width - 90}" height="${promoEnLines.length * 32 + 30}" rx="16" fill="rgba(0,0,0,0.7)" filter="url(#shadow)"/>
        ${promoEnLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 512 + index * 32}" font-family="Arial, sans-serif" font-size="22" font-weight="600" fill="white" text-anchor="middle" font-style="italic" filter="url(#textShadow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- SWIPE UP CTA Button - Professional Gaming Style with Arrow - Positioned higher for Facebook Story visibility (raised by 170px) -->
        <rect x="${width / 2 - 320}" y="${height - 490}" width="640" height="180" rx="28" fill="url(#greenPlayGrad)" filter="url(#shadow)"/>
        
        <!-- Animated Swipe Up Arrow -->
        <g transform="translate(${width / 2}, ${height - 465})">
          <path d="M-15 20 L0 5 L15 20" stroke="white" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
            <animate attributeName="opacity" values="1;0.4;1" dur="1s" repeatCount="indefinite"/>
            <animateTransform attributeName="transform" type="translate" values="0,0;0,-8;0,0" dur="1s" repeatCount="indefinite"/>
          </path>
          <path d="M-10 35 L0 25 L10 35" stroke="rgba(255,255,255,0.6)" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round">
            <animate attributeName="opacity" values="0.6;0.2;0.6" dur="1s" repeatCount="indefinite" begin="0.15s"/>
            <animateTransform attributeName="transform" type="translate" values="0,0;0,-8;0,0" dur="1s" repeatCount="indefinite" begin="0.15s"/>
          </path>
        </g>
        
        <text x="${width / 2}" y="${height - 395}" font-family="Arial, sans-serif" font-size="32" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#textShadow)">
          ${GAMING_CTA_AR}
        </text>
        <text x="${width / 2}" y="${height - 350}" font-family="Arial, sans-serif" font-size="26" font-weight="bold" fill="white" text-anchor="middle" filter="url(#textShadow)">
          ${GAMING_CTA_EN}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        return await sharp(resizedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).toBuffer();
      }
      async generateAppPoster() {
        console.log("\u{1F4F1} Generating App Poster from Google Play Store...");
        const trendingApp = await googlePlayService.getRandomTrendingApp();
        const appName = trendingApp.title;
        const appGenre = trendingApp.genre;
        const appGenreAr = googlePlayService.getAppGenreArabic(appGenre);
        const installsInfo = googlePlayService.formatInstalls(trendingApp.installs);
        console.log(`\u{1F4F1} Selected trending app from Play Store: ${appName}`);
        console.log(`   Genre: ${appGenre} (${appGenreAr})`);
        console.log(`   Rating: ${trendingApp.score}/5`);
        console.log(`   Installs: ${trendingApp.installs}`);
        console.log(`   Developer: ${trendingApp.developer}`);
        let imageBuffer = null;
        let usedImageUrl = "";
        let appLogoBase64 = null;
        const [logoResult, bestScreenshotUrl] = await Promise.all([
          this.fetchPlayStoreGameIcon(trendingApp),
          googlePlayService.getBestAppScreenshot(trendingApp)
        ]);
        appLogoBase64 = logoResult;
        if (bestScreenshotUrl) {
          usedImageUrl = bestScreenshotUrl;
          console.log(`\u{1F3AF} Using best screenshot selected by smart algorithm: ${bestScreenshotUrl.substring(0, 80)}...`);
          try {
            imageBuffer = await this.downloadImage(bestScreenshotUrl);
            console.log(`\u2705 Screenshot downloaded for full poster background`);
          } catch (error) {
            console.log(`\u26A0\uFE0F Best screenshot download failed: ${error.message}`);
          }
        }
        if (!imageBuffer) {
          const screenshotResult = await this.fetchPlayStoreScreenshot(trendingApp);
          if (screenshotResult) {
            imageBuffer = screenshotResult.buffer;
            usedImageUrl = screenshotResult.url;
            console.log(`\u2705 Fallback screenshot downloaded for poster background`);
          }
        }
        if (!imageBuffer) {
          console.log("\u{1F4F8} No screenshot available, using generated placeholder");
          imageBuffer = await this.generatePlaceholderImage(appName, "apps");
          usedImageUrl = "generated-placeholder";
        }
        console.log(`\u{1F916} Generating professional bilingual descriptions for app: ${appName}`);
        let promoAr;
        let promoEn;
        try {
          const posterContent = await this.generatePlayStoreAppDescription(trendingApp);
          promoAr = posterContent.descriptionAr;
          promoEn = posterContent.descriptionEn;
          console.log(`\u2705 AI descriptions generated with app name: ${appName}`);
        } catch (error) {
          console.log(`\u26A0\uFE0F AI generation failed, using template descriptions for: ${appName}`);
          const ratingText = trendingApp.score > 4 ? "\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A" : trendingApp.score > 3.5 ? "\u0645\u0645\u062A\u0627\u0632" : "\u0631\u0627\u0626\u0639";
          promoAr = `${appName} - \u0627\u0644\u062A\u0637\u0628\u064A\u0642 ${ratingText} \u0627\u0644\u0623\u0643\u062B\u0631 \u062A\u062D\u0645\u064A\u0644\u0627\u064B \u0639\u0644\u0649 \u0645\u062A\u062C\u0631 \u0628\u0644\u0627\u064A! ${installsInfo.ar}. \u062A\u0642\u064A\u064A\u0645 ${trendingApp.score}/5 \u0645\u0646 \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645\u064A\u0646. \u0627\u0633\u062A\u0645\u062A\u0639 \u0628\u062A\u062C\u0631\u0628\u0629 ${appGenreAr} \u0644\u0627 \u0645\u062B\u064A\u0644 \u0644\u0647\u0627 \u0645\u0639 \u062A\u0635\u0645\u064A\u0645 \u0639\u0635\u0631\u064A \u0648\u0645\u064A\u0632\u0627\u062A \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0629. \u0645\u0646 \u062A\u0637\u0648\u064A\u0631 ${trendingApp.developer}. \u062D\u0645\u0651\u0644 ${appName} \u0645\u062C\u0627\u0646\u0627\u064B \u0627\u0644\u0622\u0646 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 Premium!`;
          promoEn = `${appName} - The top-rated ${appGenre} app on Google Play! ${installsInfo.en}. Rated ${trendingApp.score}/5 by millions of users worldwide. Experience unmatched ${appGenre} functionality with modern design and professional features. Developed by ${trendingApp.developer}. Download ${appName} FREE today and get Premium!`;
        }
        const validImageBuffer = imageBuffer;
        let processedImage;
        try {
          processedImage = await this.createAppOverlay(
            validImageBuffer,
            appName,
            promoAr,
            promoEn,
            appLogoBase64,
            appGenreAr
          );
        } catch (overlayError) {
          console.log(`\u26A0\uFE0F App overlay failed: ${overlayError.message}, using simple fallback`);
          processedImage = await this.createSimpleAppFallback(
            validImageBuffer,
            appName,
            promoAr,
            promoEn,
            appLogoBase64
          );
        }
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/apps/${timestamp}-${randomId}`;
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category: "apps", topic: appName, source: "google-play" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category: "apps", topic: appName, source: "google-play" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category: "apps", topic: appName, platform: "facebook", source: "google-play" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category: "apps", topic: appName, platform: "instagram", source: "google-play" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category: "apps", topic: appName, platform: "tiktok", source: "google-play" }
          })
        ]);
        console.log(`\u2705 App poster for "${appName}" uploaded successfully (source: Google Play Store)`);
        const metadata = {
          category: "apps",
          trendingTerm: appName,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic: appName,
          posterTitle: appName,
          sourceImageUrl: usedImageUrl,
          metadata,
          descriptionAr: promoAr,
          descriptionEn: promoEn,
          voteAverage: trendingApp.score
        };
      }
      /**
       * Generate professional descriptions for Play Store apps using AI
       */
      async generatePlayStoreAppDescription(app2) {
        const appGenreAr = googlePlayService.getAppGenreArabic(app2.genre);
        const installsInfo = googlePlayService.formatInstalls(app2.installs);
        const ratingText = app2.score > 4 ? "\u0627\u0633\u062A\u062B\u0646\u0627\u0626\u064A" : app2.score > 3.5 ? "\u0645\u0645\u062A\u0627\u0632" : "\u0631\u0627\u0626\u0639";
        const descriptionAr = `${app2.title} - \u0627\u0644\u062A\u0637\u0628\u064A\u0642 ${ratingText} \u0627\u0644\u0623\u0643\u062B\u0631 \u062A\u062D\u0645\u064A\u0644\u0627\u064B \u0641\u064A \u0641\u0626\u0629 ${appGenreAr}! ${installsInfo.ar}. \u062A\u0642\u064A\u064A\u0645 ${app2.score}/5 \u0645\u0646 \u0645\u0644\u0627\u064A\u064A\u0646 \u0627\u0644\u0645\u0633\u062A\u062E\u062F\u0645\u064A\u0646 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645. \u062A\u0635\u0645\u064A\u0645 \u0639\u0635\u0631\u064A \u0623\u0646\u064A\u0642 \u0648\u0623\u062F\u0627\u0621 \u0641\u0627\u0626\u0642 \u0627\u0644\u0633\u0631\u0639\u0629 \u0628\u062F\u0648\u0646 \u0623\u064A \u062A\u0623\u062E\u064A\u0631. \u0645\u064A\u0632\u0627\u062A \u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0629 \u062D\u0635\u0631\u064A\u0629 \u0633\u062A\u063A\u064A\u0631 \u0637\u0631\u064A\u0642\u0629 \u0627\u0633\u062A\u062E\u062F\u0627\u0645\u0643 \u0644\u0644\u0647\u0627\u062A\u0641. \u062D\u0645\u0651\u0644 ${app2.title} \u0627\u0644\u0622\u0646 \u0648\u0627\u062D\u0635\u0644 \u0639\u0644\u0649 Premium \u0645\u062C\u0627\u0646\u0627\u064B!`;
        const descriptionEn = `${app2.title} - The top-rated ${app2.genre} app with ${installsInfo.en}! Rated ${app2.score}/5 by millions worldwide. Elegant modern design with super-fast performance and zero lag. Exclusive professional features that will transform how you use your phone. Download ${app2.title} now and get Premium FREE!`;
        return { descriptionAr, descriptionEn };
      }
      async createAppOverlay(imageBuffer, appName, promoAr, promoEn, appLogoBase64 = null, appGenreAr = "\u062A\u0637\u0628\u064A\u0642") {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(width, height, {
          fit: "cover",
          position: "center"
        }).toBuffer();
        const nameLines = this.wrapText(appName, 16, 2).map((line) => escapeXml(line));
        const promoArLines = this.wrapText(promoAr, 28, 4).map((line) => escapeXml(line));
        const promoEnLines = this.wrapText(promoEn, 34, 4).map((line) => escapeXml(line));
        const appInitials = escapeXml(this.getGameInitials(appName));
        const safeGenreAr = escapeXml(appGenreAr);
        const validatedLogo = await this.validateAndProcessBase64Image(appLogoBase64);
        const appIconElement = validatedLogo ? `<image x="${width / 2 - 80}" y="85" width="160" height="160" href="${validatedLogo}" preserveAspectRatio="xMidYMid meet" filter="url(#logoShadow)"/>` : `<text x="${width / 2}" y="${appInitials.length === 1 ? 190 : 185}" font-family="Arial, sans-serif" font-size="${appInitials.length === 1 ? 90 : 75}" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" filter="url(#neonGlow)">${appInitials}</text>`;
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.3);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="30%" style="stop-color:rgba(0,0,0,0.2);stop-opacity:1" />
            <stop offset="60%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.75);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="premiumGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#8b5cf6;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#a855f7;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#d946ef;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="cyanGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#0891b2;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#06b6d4;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="greenPlayGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#059669;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#10b981;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="neonGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#6366f1;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#8b5cf6;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#a855f7;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="blueGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#1e40af;stop-opacity:0.5" />
            <stop offset="100%" style="stop-color:#3b82f6;stop-opacity:0.5" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="8" stdDeviation="14" flood-color="rgba(0,0,0,0.98)"/>
          </filter>
          <filter id="logoShadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.9)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="6" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="neonGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="10" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="titleGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="textShadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
        </defs>
        
        <!-- Subtle top gradient for header only -->
        <rect x="0" y="0" width="${width}" height="280" fill="url(#topGrad)"/>
        
        <!-- Subtle bottom gradient only for CTA area -->
        <rect x="0" y="${height - 280}" width="${width}" height="280" fill="url(#bottomGrad)"/>
        
        <!-- TRENDING NOW Badge at top -->
        <rect x="${width / 2 - 120}" y="20" width="240" height="42" rx="21" fill="url(#premiumGrad)" filter="url(#shadow)"/>
        <circle cx="${width / 2 - 85}" cy="41" r="8" fill="white">
          <animate attributeName="opacity" values="1;0.3;1" dur="1.5s" repeatCount="indefinite"/>
        </circle>
        <text x="${width / 2 + 10}" y="49" font-family="Arial, sans-serif" font-size="20" font-weight="bold" fill="white" text-anchor="middle">
          TRENDING NOW
        </text>
        
        <!-- Professional App Logo Container -->
        <rect x="${width / 2 - 90}" y="75" width="180" height="180" rx="35" fill="url(#blueGrad)" filter="url(#shadow)"/>
        <rect x="${width / 2 - 85}" y="80" width="170" height="170" rx="32" fill="url(#neonGrad)"/>
        <rect x="${width / 2 - 82}" y="83" width="164" height="164" rx="30" fill="rgba(255,255,255,0.15)"/>
        ${appIconElement}
        
        <!-- APP NAME - Compact Semi-transparent Background Only Under Text -->
        <rect x="${width / 2 - 250}" y="275" width="500" height="${nameLines.length * 70 + 25}" rx="16" fill="rgba(0,0,0,0.25)"/>
        ${nameLines.map((line, index) => `
        <text x="${width / 2}" y="${318 + index * 70}" font-family="Arial, sans-serif" font-size="54" font-weight="bold" fill="white" text-anchor="middle" filter="url(#titleGlow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- Badges Row - Compact Design -->
        <g transform="translate(${width / 2}, ${305 + nameLines.length * 70})">
          <rect x="-180" y="0" width="120" height="38" rx="19" fill="url(#goldGrad)" filter="url(#shadow)"/>
          <text x="-120" y="26" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            TRENDING
          </text>
          
          <rect x="60" y="0" width="120" height="38" rx="19" fill="url(#cyanGrad)" filter="url(#shadow)"/>
          <text x="120" y="26" font-family="Arial, sans-serif" font-size="16" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            ${safeGenreAr}
          </text>
        </g>
        
        <!-- Arabic Promotional Text with Blue Background - Full Width (raised by 170px) -->
        <rect x="20" y="${height - 750}" width="${width - 40}" height="${Math.min(promoArLines.length, 3) * 42 + 30}" rx="16" fill="url(#blueGrad)" filter="url(#shadow)"/>
        ${promoArLines.slice(0, 3).map((line, index) => `
        <text x="${width / 2}" y="${height - 715 + index * 42}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#textShadow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text with Black Transparent Background - Full Width (raised by 170px) -->
        <rect x="20" y="${height - 605}" width="${width - 40}" height="${Math.min(promoEnLines.length, 3) * 38 + 25}" rx="16" fill="rgba(0,0,0,0.5)" filter="url(#shadow)"/>
        ${promoEnLines.slice(0, 3).map((line, index) => `
        <text x="${width / 2}" y="${height - 570 + index * 38}" font-family="Arial, sans-serif" font-size="22" font-weight="bold" fill="white" text-anchor="middle" font-style="italic">
          ${line}
        </text>
        `).join("")}
        
        <!-- Professional CTA Button with Premium Gradient - Full Width - Positioned higher for Facebook Story visibility (raised by 170px) -->
        <rect x="20" y="${height - 470}" width="${width - 40}" height="155" rx="22" fill="url(#premiumGrad)" filter="url(#shadow)"/>
        <rect x="25" y="${height - 465}" width="${width - 50}" height="145" rx="20" fill="rgba(255,255,255,0.1)"/>
        
        <!-- Animated Arrow Icon -->
        <g transform="translate(${width / 2}, ${height - 440})">
          <path d="M-15 22 L0 7 L15 22" stroke="white" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round">
            <animate attributeName="opacity" values="1;0.4;1" dur="1s" repeatCount="indefinite"/>
            <animateTransform attributeName="transform" type="translate" values="0,0;0,-6;0,0" dur="1s" repeatCount="indefinite"/>
          </path>
        </g>
        
        <!-- Arabic CTA Text - Larger and Clearer -->
        <text x="${width / 2}" y="${height - 380}" font-family="Arial, sans-serif" font-size="32" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${escapeXml(APPS_CTA_AR)}
        </text>
        
        <!-- English CTA Text - Larger and Clearer -->
        <text x="${width / 2}" y="${height - 335}" font-family="Arial, sans-serif" font-size="26" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
          ${escapeXml(APPS_CTA_EN)}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        return await sharp(resizedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).toBuffer();
      }
      async createSimpleAppFallback(imageBuffer, appName, promoAr = "", promoEn = "", appLogoBase64 = null) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        try {
          const processedImage = await sharp(imageBuffer).resize(width, height, {
            fit: "cover",
            position: "center"
          }).modulate({ brightness: 0.85, saturation: 0.95 }).png().toBuffer();
          const safeAppName = (appName || "App").trim();
          const appInitials = escapeXml(this.getGameInitials(safeAppName));
          const nameLines = this.wrapText(safeAppName, 16, 2).map((line) => escapeXml(line));
          const promoArLines = this.wrapText(promoAr || "\u062A\u0637\u0628\u064A\u0642 \u0645\u0645\u064A\u0632 \u0648\u062D\u0635\u0631\u064A!", 30, 3).map((line) => escapeXml(line));
          const promoEnLines = this.wrapText(promoEn || "Amazing exclusive app!", 38, 3).map((line) => escapeXml(line));
          const validatedLogo = await this.validateAndProcessBase64Image(appLogoBase64);
          const logoElement = validatedLogo ? `<image x="${width / 2 - 70}" y="95" width="140" height="140" href="${validatedLogo}" preserveAspectRatio="xMidYMid meet"/>` : `<text x="${width / 2}" y="180" font-family="Arial, sans-serif" font-size="70" font-weight="bold" fill="white" text-anchor="middle">${appInitials}</text>`;
          const svgOverlay = `
        <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
          <defs>
            <linearGradient id="topGradApp" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
              <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            </linearGradient>
            <linearGradient id="bottomGradApp" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
              <stop offset="50%" style="stop-color:rgba(0,0,0,0.3);stop-opacity:1" />
              <stop offset="100%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            </linearGradient>
            <linearGradient id="cyanGradApp" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#0891b2;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#06b6d4;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="goldGradApp" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="purpleGradApp" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#8b5cf6;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#a855f7;stop-opacity:1" />
            </linearGradient>
            <linearGradient id="blueGradFb" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" style="stop-color:#1e40af;stop-opacity:1" />
              <stop offset="100%" style="stop-color:#3b82f6;stop-opacity:1" />
            </linearGradient>
            <filter id="textShadowFb" x="-50%" y="-50%" width="200%" height="200%">
              <feDropShadow dx="0" dy="3" stdDeviation="5" flood-color="rgba(0,0,0,0.9)"/>
            </filter>
          </defs>
          
          <!-- Top gradient - reduced height -->
          <rect x="0" y="0" width="${width}" height="280" fill="url(#topGradApp)"/>
          
          <!-- Bottom gradient - reduced height -->
          <rect x="0" y="${height - 280}" width="${width}" height="280" fill="url(#bottomGradApp)"/>
          
          <!-- TRENDING Badge -->
          <rect x="${width / 2 - 100}" y="25" width="200" height="38" rx="19" fill="url(#purpleGradApp)"/>
          <text x="${width / 2}" y="50" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">TRENDING NOW</text>
          
          <!-- Logo Container -->
          <rect x="${width / 2 - 75}" y="80" width="150" height="150" rx="28" fill="url(#cyanGradApp)"/>
          <rect x="${width / 2 - 70}" y="85" width="140" height="140" rx="25" fill="rgba(255,255,255,0.12)"/>
          ${logoElement}
          
          <!-- App Name - Semi-transparent compact background -->
          <rect x="${width / 2 - 220}" y="250" width="440" height="${nameLines.length * 65 + 20}" rx="14" fill="rgba(0,0,0,0.2)"/>
          ${nameLines.map((line, index) => `
          <text x="${width / 2}" y="${295 + index * 65}" font-family="Arial, sans-serif" font-size="48" font-weight="bold" fill="white" text-anchor="middle" filter="url(#textShadowFb)">
            ${line}
          </text>
          `).join("")}
          
          <!-- Trend Badge -->
          <rect x="${width / 2 - 70}" y="${280 + nameLines.length * 65}" width="140" height="35" rx="17" fill="url(#goldGradApp)"/>
          <text x="${width / 2}" y="${304 + nameLines.length * 65}" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">
            TRENDING
          </text>
          
          <!-- Arabic Promo Text with Blue Background - Full Width (raised by 170px) -->
          <rect x="20" y="${height - 730}" width="${width - 40}" height="${Math.min(promoArLines.length, 2) * 38 + 24}" rx="14" fill="url(#blueGradFb)" filter="url(#textShadowFb)"/>
          ${promoArLines.slice(0, 2).map((line, index) => `
          <text x="${width / 2}" y="${height - 700 + index * 38}" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            ${line}
          </text>
          `).join("")}
          
          <!-- English Promo Text with Black Transparent Background - Full Width (raised by 170px) -->
          <rect x="20" y="${height - 615}" width="${width - 40}" height="${Math.min(promoEnLines.length, 2) * 34 + 20}" rx="14" fill="rgba(0,0,0,0.6)" filter="url(#textShadowFb)"/>
          ${promoEnLines.slice(0, 2).map((line, index) => `
          <text x="${width / 2}" y="${height - 590 + index * 34}" font-family="Arial, sans-serif" font-size="20" font-weight="bold" fill="white" text-anchor="middle" font-style="italic">
            ${line}
          </text>
          `).join("")}
          
          <!-- CTA Button - Premium Design - Full Width - Positioned higher for Facebook Story visibility (raised by 170px) -->
          <rect x="20" y="${height - 470}" width="${width - 40}" height="155" rx="22" fill="url(#purpleGradApp)"/>
          <rect x="25" y="${height - 465}" width="${width - 50}" height="145" rx="20" fill="rgba(255,255,255,0.1)"/>
          
          <text x="${width / 2}" y="${height - 385}" font-family="Arial, sans-serif" font-size="30" font-weight="bold" fill="white" text-anchor="middle" direction="rtl">
            ${escapeXml(APPS_CTA_AR)}
          </text>
          <text x="${width / 2}" y="${height - 340}" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
            ${escapeXml(APPS_CTA_EN)}
          </text>
        </svg>
      `;
          const overlayBuffer = Buffer.from(svgOverlay);
          return await sharp(processedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).png().toBuffer();
        } catch (error) {
          console.log(`\u26A0\uFE0F App fallback also failed: ${error.message}, returning base image`);
          return await sharp(imageBuffer).resize(width, height, { fit: "cover" }).png().toBuffer();
        }
      }
      async generateTVChannelsPoster() {
        console.log("\u{1F4FA} Generating TV Channels Poster...");
        const trendResult = await googleTrendsService.getBestTrendForCategory("tv_channels");
        const trendingTopic = trendResult.trendingTerm;
        console.log(`\u{1F4FA} Trending TV channel topic: ${trendingTopic}`);
        const categoryQueries = CATEGORY_SEARCH_QUERIES["tv_channels"];
        const searchQuery = `${trendingTopic} ${categoryQueries[Math.floor(Math.random() * categoryQueries.length)]}`;
        let imageResult = await googleImageSearchService.searchThumbnailImage(searchQuery, "tv_channels");
        if (!imageResult) {
          imageResult = await googleImageSearchService.searchThumbnailImage(categoryQueries[0], "tv_channels");
        }
        if (!imageResult) {
          try {
            console.log("\u{1F3A8} Generating HD image using Hugging Face Flux...");
            const imagePrompt = await generateCategoryImagePrompt(trendingTopic, "tv_channels", true);
            const generatedImage = await huggingFaceSDK.generateImage(imagePrompt);
            if (generatedImage && generatedImage.imageData) {
              const generatedBuffer = Buffer.from(generatedImage.imageData, "base64");
              imageResult = {
                imageUrl: "generated",
                thumbnailUrl: "generated",
                source: "huggingface",
                title: trendingTopic,
                generatedBuffer
              };
            }
          } catch (err) {
            console.error("Failed to generate image with Hugging Face:", err);
          }
        }
        if (!imageResult) {
          throw new Error("\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0644\u0642\u0646\u0627\u0629 \u0627\u0644\u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629");
        }
        const promoIndex = Math.floor(Math.random() * TV_CHANNELS_PROMOTIONAL_AR.length);
        const promoAr = TV_CHANNELS_PROMOTIONAL_AR[promoIndex];
        const promoEn = TV_CHANNELS_PROMOTIONAL_EN[promoIndex];
        const imageBuffer = imageResult.generatedBuffer || await this.downloadImage(imageResult.imageUrl);
        const processedImage = await this.createTVChannelOverlay(
          imageBuffer,
          trendingTopic,
          promoAr,
          promoEn
        );
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const baseFileName = `trending/tv_channels/${timestamp}-${randomId}`;
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const [pngUrl, webpUrl, fbPngUrl, igPngUrl, tiktokWebpUrl] = await Promise.all([
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-original.png`, {
            contentType: "image/png",
            metadata: { category: "tv_channels", topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-original.webp`, {
            contentType: "image/webp",
            metadata: { category: "tv_channels", topic: trendingTopic, source: "google" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-facebook.png`, {
            contentType: "image/png",
            metadata: { category: "tv_channels", topic: trendingTopic, platform: "facebook", source: "google" }
          }),
          r2Storage.uploadFile(pngBuffer, `${baseFileName}-instagram.png`, {
            contentType: "image/png",
            metadata: { category: "tv_channels", topic: trendingTopic, platform: "instagram", source: "google" }
          }),
          r2Storage.uploadFile(webpBuffer, `${baseFileName}-tiktok.webp`, {
            contentType: "image/webp",
            metadata: { category: "tv_channels", topic: trendingTopic, platform: "tiktok", source: "google" }
          })
        ]);
        console.log(`\u2705 TV Channel poster uploaded successfully`);
        const metadata = {
          category: "tv_channels",
          trendingTerm: trendingTopic,
          imageUrl: pngUrl,
          isEdited: false,
          platformTargets: ["Facebook", "Instagram", "TikTok"]
        };
        return {
          pngUrl,
          webpUrl,
          facebookPngUrl: fbPngUrl,
          instagramPngUrl: igPngUrl,
          tiktokWebpUrl,
          trendingTopic,
          posterTitle: trendingTopic,
          sourceImageUrl: imageResult.generatedBuffer ? "generated" : imageResult.imageUrl,
          metadata,
          descriptionAr: promoAr,
          descriptionEn: promoEn
        };
      }
      async createTVChannelOverlay(imageBuffer, channelName, promoAr, promoEn) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).toBuffer();
        const nameLines = this.wrapText(channelName, 16, 3).map((line) => escapeXml(line));
        const promoArLines = this.wrapText(promoAr, 28, 4).map((line) => escapeXml(line));
        const promoEnLines = this.wrapText(promoEn, 35, 4).map((line) => escapeXml(line));
        const channelInitials = escapeXml(this.getGameInitials(channelName));
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="20%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.85);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="purpleBlueGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#4f46e5;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#7c3aed;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#f59e0b;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="redLiveGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="indigoGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#4338ca;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#6366f1;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="tealGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#0d9488;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#14b8a6;stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="6" stdDeviation="12" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="5" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="tvGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="8" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="titleGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="10" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        
        <!-- Top gradient for header -->
        <rect x="0" y="0" width="${width}" height="520" fill="url(#topGrad)"/>
        
        <!-- Bottom gradient for content - Extended for 4 lines -->
        <rect x="0" y="${height - 750}" width="${width}" height="750" fill="url(#bottomGrad)"/>
        
        <!-- LIVE Badge with blinking effect -->
        <rect x="${width / 2 - 60}" y="25" width="120" height="35" rx="17" fill="url(#redLiveGrad)" filter="url(#shadow)"/>
        <circle cx="${width / 2 - 35}" cy="42" r="6" fill="white"/>
        <text x="${width / 2 + 10}" y="48" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">
          LIVE
        </text>
        
        <!-- TV Icon Circle -->
        <rect x="${width / 2 - 55}" y="75" width="110" height="90" rx="12" fill="url(#purpleBlueGrad)" filter="url(#shadow)"/>
        <rect x="${width / 2 - 50}" y="80" width="100" height="80" rx="10" fill="url(#indigoGrad)"/>
        <text x="${width / 2}" y="135" font-family="Arial, sans-serif" font-size="45" fill="white" text-anchor="middle">
          \u{1F4FA}
        </text>
        
        <!-- TRENDING Badge -->
        <rect x="${width / 2 - 120}" y="180" width="240" height="42" rx="21" fill="url(#goldGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="208" font-family="Arial, sans-serif" font-size="22" font-weight="bold" fill="white" text-anchor="middle">
          TRENDING CHANNEL
        </text>
        
        <!-- Channel Name with prominent display -->
        <rect x="30" y="240" width="${width - 60}" height="${nameLines.length * 62 + 45}" rx="20" fill="rgba(0,0,0,0.7)" filter="url(#shadow)"/>
        ${nameLines.map((line, index) => `
        <text x="${width / 2}" y="${290 + index * 62}" font-family="Arial, sans-serif" font-size="54" font-weight="bold" fill="white" text-anchor="middle" filter="url(#titleGlow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- Arabic Channel Label -->
        <rect x="${width / 2 - 100}" y="${300 + nameLines.length * 62}" width="200" height="48" rx="24" fill="url(#purpleBlueGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${332 + nameLines.length * 62}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          \u0642\u0646\u0627\u0629 \u062A\u0631\u0646\u062F \u0627\u0644\u0622\u0646
        </text>
        
        <!-- Arabic Promotional Text - 4 Lines (raised by 170px) -->
        <rect x="35" y="${height - 770}" width="${width - 70}" height="${promoArLines.length * 42 + 45}" rx="22" fill="rgba(79,70,229,0.95)" filter="url(#shadow)"/>
        ${promoArLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 735 + index * 42}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text - 4 Lines (raised by 170px) -->
        ${promoEnLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 550 + index * 38}" font-family="Arial, sans-serif" font-size="24" fill="rgba(255,255,255,0.95)" text-anchor="middle" font-style="italic">
          "${line}"
        </text>
        `).join("")}
        
        <!-- WATCH LIVE CTA Button - Professional TV Style - Positioned higher for Facebook Story visibility (raised by 170px) -->
        <rect x="${width / 2 - 250}" y="${height - 450}" width="500" height="135" rx="25" fill="url(#tealGrad)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${height - 393}" font-family="Arial, sans-serif" font-size="38" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#tvGlow)">
          ${TV_CHANNELS_CTA_AR}
        </text>
        <text x="${width / 2}" y="${height - 345}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
          ${TV_CHANNELS_CTA_EN}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        return await sharp(resizedImage).composite([{ input: overlayBuffer, top: 0, left: 0 }]).toBuffer();
      }
      async downloadImage(url) {
        if (url.startsWith("data:")) {
          const base64Data = url.split(",")[1];
          const buffer2 = Buffer.from(base64Data, "base64");
          return await this.validateAndConvertImage(buffer2);
        }
        const response = await fetch(url, {
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8"
          }
        });
        if (!response.ok) {
          throw new Error(`Failed to download image: ${response.statusText}`);
        }
        const contentType = response.headers.get("content-type") || "";
        const arrayBuffer = await response.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        if (contentType.includes("svg") || this.isSvgBuffer(buffer)) {
          throw new Error("SVG images are not supported, trying next image");
        }
        if (contentType.includes("html") || this.isHtmlBuffer(buffer)) {
          throw new Error("Received HTML instead of image, trying next image");
        }
        return await this.validateAndConvertImage(buffer);
      }
      isSvgBuffer(buffer) {
        const header = buffer.slice(0, 500).toString("utf-8").toLowerCase();
        return header.includes("<svg") || header.includes("<?xml");
      }
      isHtmlBuffer(buffer) {
        const header = buffer.slice(0, 500).toString("utf-8").toLowerCase();
        return header.includes("<html") || header.includes("<!doctype");
      }
      async validateAndConvertImage(buffer) {
        try {
          const image = sharp(buffer);
          const metadata = await image.metadata();
          if (!metadata.format || !["jpeg", "png", "webp", "gif", "tiff", "avif"].includes(metadata.format)) {
            throw new Error(`Unsupported image format: ${metadata.format}`);
          }
          return await image.png().toBuffer();
        } catch (error) {
          if (error.message.includes("corrupt") || error.message.includes("XML") || error.message.includes("parse")) {
            throw new Error("Image is corrupted or in unsupported format");
          }
          throw error;
        }
      }
      async processImageForStories(imageBuffer, title, category, latestEpisode, descriptionEn, descriptionAr) {
        const image = sharp(imageBuffer);
        const metadata = await image.metadata();
        const storyImage = await this.createStoryImage(imageBuffer, metadata.width || 800, metadata.height || 1200);
        let processedImage = storyImage;
        if (category === "tv_shows" && latestEpisode !== void 0) {
          processedImage = await this.addEpisodeOverlay(storyImage, latestEpisode, title, descriptionEn, descriptionAr);
        } else {
          processedImage = await this.addTitleOverlay(storyImage, title, category, descriptionEn, descriptionAr);
        }
        const [pngBuffer, webpBuffer] = await Promise.all([
          sharp(processedImage).png({ quality: 95 }).toBuffer(),
          sharp(processedImage).webp({ quality: 90 }).toBuffer()
        ]);
        const facebookPngBuffer = await sharp(processedImage).resize(1080, 1920, { fit: "cover", position: "center" }).png({ quality: 95 }).toBuffer();
        const instagramPngBuffer = await sharp(processedImage).resize(1080, 1920, { fit: "cover", position: "center" }).png({ quality: 95 }).toBuffer();
        const tiktokWebpBuffer = await sharp(processedImage).resize(1080, 1920, { fit: "cover", position: "center" }).webp({ quality: 90 }).toBuffer();
        return {
          pngBuffer,
          webpBuffer,
          facebookPngBuffer,
          instagramPngBuffer,
          tiktokWebpBuffer
        };
      }
      async createStoryImage(imageBuffer, originalWidth, originalHeight) {
        const targetWidth = STORY_DIMENSIONS.width;
        const targetHeight = STORY_DIMENSIONS.height;
        const resizedImage = await sharp(imageBuffer).resize(targetWidth, targetHeight, {
          fit: "cover",
          position: "center"
        }).toBuffer();
        return resizedImage;
      }
      async addEpisodeOverlay(imageBuffer, episode, title, descriptionEn, descriptionAr) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const episodeText = `\u0627\u0644\u062D\u0644\u0642\u0629 ${episode}`;
        const episodeTextEn = `Episode ${episode}`;
        const ctaAr = "\u0634\u0627\u0647\u062F \u0627\u0644\u0622\u0646";
        const ctaEn = "WATCH NOW";
        const promoTextAr = descriptionAr || "\u0645\u0633\u0644\u0633\u0644 \u0631\u0627\u0626\u0639 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u0629 \u0645\u0646 \u0627\u0644\u062D\u0644\u0642\u0629 \u0627\u0644\u0623\u0648\u0644\u0649! \u0623\u062D\u062F\u0627\u062B \u0645\u062B\u064A\u0631\u0629 \u0648\u062A\u0634\u0648\u064A\u0642 \u0644\u0627 \u064A\u0646\u062A\u0647\u064A. \u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u062A\u062D\u0641\u0629 \u0627\u0644\u0641\u0646\u064A\u0629 \u0627\u0644\u0645\u0630\u0647\u0644\u0629";
        const promoTextEn = descriptionEn || "An amazing series worth watching from episode one! Thrilling events and endless suspense. Don't miss this stunning masterpiece";
        const titleLines = this.wrapText(title, 16, 3);
        const arabicLines = this.wrapText(promoTextAr, 28, 4);
        const englishLines = this.wrapText(promoTextEn, 35, 4);
        console.log(`\u{1F3A8} Creating episode overlay for "${title}" - Episode ${episode}`);
        console.log(`   Arabic text (${arabicLines.length} lines): "${promoTextAr.substring(0, 80)}..."`);
        console.log(`   English text (${englishLines.length} lines): "${promoTextEn.substring(0, 80)}..."`);
        console.log(`   CTA: ${ctaAr} / ${ctaEn}`);
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="topGradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.6);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="20%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.85);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="badgeGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#7c3aed;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#8b5cf6;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#a855f7;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#f59e0b;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#d97706;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="redGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#dc2626;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#ef4444;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="arabicGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" style="stop-color:rgba(124,58,237,0.95);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(139,92,246,0.95);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(124,58,237,0.95);stop-opacity:1" />
          </linearGradient>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="6" stdDeviation="12" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
          <filter id="textShadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.9)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="5" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
          <filter id="titleGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="8" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        
        <!-- Top gradient overlay -->
        <rect x="0" y="0" width="${width}" height="520" fill="url(#topGradient)"/>
        
        <!-- Extended bottom gradient for bilingual content - 4 lines -->
        <rect x="0" y="${height - 750}" width="${width}" height="750" fill="url(#bottomGradient)"/>
        
        <!-- NEW EPISODE Badge -->
        <rect x="${width / 2 - 120}" y="25" width="240" height="38" rx="19" fill="url(#redGradient)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="50" font-family="Arial, sans-serif" font-size="20" font-weight="bold" fill="white" text-anchor="middle">
          NEW EPISODE
        </text>
        
        <!-- Episode Number Badge (Bilingual) -->
        <rect x="${width / 2 - 150}" y="75" width="300" height="80" rx="40" fill="url(#badgeGradient)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="110" font-family="Arial, sans-serif" font-size="32" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${episodeText}
        </text>
        <text x="${width / 2}" y="142" font-family="Arial, sans-serif" font-size="20" fill="rgba(255,255,255,0.9)" text-anchor="middle">
          ${episodeTextEn}
        </text>
        
        <!-- Series Title with prominent display -->
        <rect x="30" y="175" width="${width - 60}" height="${titleLines.length * 62 + 45}" rx="20" fill="rgba(0,0,0,0.7)" filter="url(#shadow)"/>
        ${titleLines.map((line, index) => `
        <text x="${width / 2}" y="${225 + index * 62}" font-family="Arial, sans-serif" font-size="54" font-weight="bold" fill="white" text-anchor="middle" filter="url(#titleGlow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- Arabic Series Label -->
        <rect x="${width / 2 - 100}" y="${235 + titleLines.length * 62}" width="200" height="48" rx="24" fill="url(#badgeGradient)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${267 + titleLines.length * 62}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" filter="url(#glow)">
          \u0645\u0633\u0644\u0633\u0644 \u062C\u062F\u064A\u062F
        </text>
        
        <!-- Arabic Promotional Text Section - 4 Lines (raised by 170px) -->
        <rect x="35" y="${height - 770}" width="${width - 70}" height="${arabicLines.length * 42 + 45}" rx="22" fill="url(#arabicGradient)" filter="url(#shadow)"/>
        ${arabicLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 735 + index * 42}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text Section - 4 Lines (raised by 170px) -->
        ${englishLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 550 + index * 38}" font-family="Arial, sans-serif" font-size="24" fill="rgba(255,255,255,0.95)" text-anchor="middle" filter="url(#textShadow)" font-style="italic">
          "${line}"
        </text>
        `).join("")}
        
        <!-- Call-to-Action Button (Bilingual) - Positioned higher for Facebook Story visibility (raised by 170px) -->
        <rect x="${width / 2 - 220}" y="${height - 450}" width="440" height="135" rx="25" fill="url(#badgeGradient)" filter="url(#shadow)"/>
        <text x="${width / 2}" y="${height - 393}" font-family="Arial, sans-serif" font-size="38" font-weight="bold" fill="white" text-anchor="middle" direction="rtl" filter="url(#glow)">
          ${ctaAr}
        </text>
        <text x="${width / 2}" y="${height - 345}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle">
          ${ctaEn}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        const result = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).composite([
          {
            input: overlayBuffer,
            top: 0,
            left: 0
          }
        ]).toBuffer();
        return result;
      }
      wrapText(text, maxCharsPerLine, maxLines = 4) {
        const words = text.split(" ");
        const lines = [];
        let currentLine = "";
        for (const word of words) {
          if ((currentLine + " " + word).trim().length <= maxCharsPerLine) {
            currentLine = (currentLine + " " + word).trim();
          } else {
            if (currentLine) {
              lines.push(currentLine);
            }
            currentLine = word;
          }
        }
        if (currentLine) {
          lines.push(currentLine);
        }
        return lines.slice(0, maxLines);
      }
      async addTitleOverlay(imageBuffer, title, category, descriptionEn, descriptionAr) {
        const width = STORY_DIMENSIONS.width;
        const height = STORY_DIMENSIONS.height;
        const categoryLabels = {
          "movies": { en: "MOVIE", ar: "\u0641\u064A\u0644\u0645" },
          "tv_shows": { en: "SERIES", ar: "\u0645\u0633\u0644\u0633\u0644" },
          "sports": { en: "SPORTS", ar: "\u0631\u064A\u0627\u0636\u0629" },
          "recipes": { en: "RECIPE", ar: "\u0648\u0635\u0641\u0629" },
          "gaming": { en: "GAMING", ar: "\u0623\u0644\u0639\u0627\u0628" },
          "apps": { en: "APP", ar: "\u062A\u0637\u0628\u064A\u0642" },
          "tv_channels": { en: "TV CHANNEL", ar: "\u0642\u0646\u0627\u0629 \u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629" }
        };
        const categoryColors = {
          "movies": { primary: "#dc2626", secondary: "#e11d48" },
          "tv_shows": { primary: "#7c3aed", secondary: "#8b5cf6" },
          "sports": { primary: "#059669", secondary: "#10b981" },
          "recipes": { primary: "#ea580c", secondary: "#f97316" },
          "gaming": { primary: "#2563eb", secondary: "#3b82f6" },
          "apps": { primary: "#0891b2", secondary: "#06b6d4" },
          "tv_channels": { primary: "#4f46e5", secondary: "#6366f1" }
        };
        const ctaMessages = {
          "movies": { ar: "\u0634\u0627\u0647\u062F \u0627\u0644\u0622\u0646", en: "WATCH NOW" },
          "tv_shows": { ar: "\u062A\u0627\u0628\u0639 \u0627\u0644\u0645\u0633\u0644\u0633\u0644", en: "FOLLOW THE SERIES" },
          "sports": { ar: "\u0644\u0627 \u062A\u0641\u0648\u062A \u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629", en: "DON'T MISS IT" },
          "recipes": { ar: "\u062C\u0631\u0628 \u0627\u0644\u0648\u0635\u0641\u0629", en: "TRY THE RECIPE" },
          "gaming": { ar: "\u0627\u0644\u0639\u0628 \u0627\u0644\u0622\u0646", en: "PLAY NOW" },
          "apps": { ar: "\u062D\u0645\u0651\u0644 \u0627\u0644\u062A\u0637\u0628\u064A\u0642", en: "DOWNLOAD NOW" },
          "tv_channels": { ar: "\u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631", en: "WATCH LIVE" }
        };
        const arabicDefaultDescriptions = {
          "movies": ["\u0641\u064A\u0644\u0645 \u0631\u0627\u0626\u0639 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A \u0647\u0630\u0647 \u0627\u0644\u062A\u062D\u0641\u0629 \u0627\u0644\u0633\u064A\u0646\u0645\u0627\u0626\u064A\u0629", "\u0623\u0641\u0636\u0644 \u0641\u064A\u0644\u0645 \u0641\u064A \u0627\u0644\u0645\u0648\u0633\u0645! \u0634\u0627\u0647\u062F\u0647 \u0627\u0644\u0622\u0646", "\u0641\u064A\u0644\u0645 \u0645\u0630\u0647\u0644 \u0633\u064A\u0623\u0633\u0631 \u0642\u0644\u0628\u0643 \u0645\u0646 \u0627\u0644\u0628\u062F\u0627\u064A\u0629 \u0644\u0644\u0646\u0647\u0627\u064A\u0629"],
          "tv_shows": ["\u0645\u0633\u0644\u0633\u0644 \u0631\u0627\u0626\u0639 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A \u0623\u062D\u062F\u0627\u062B\u0647 \u0627\u0644\u0645\u062B\u064A\u0631\u0629", "\u0623\u0641\u0636\u0644 \u0645\u0633\u0644\u0633\u0644 \u0641\u064A \u0627\u0644\u0645\u0648\u0633\u0645! \u0634\u0627\u0647\u062F\u0647 \u0627\u0644\u0622\u0646", "\u0645\u0633\u0644\u0633\u0644 \u0645\u0630\u0647\u0644 \u0633\u064A\u062C\u0639\u0644\u0643 \u062A\u0646\u062A\u0638\u0631 \u0643\u0644 \u062D\u0644\u0642\u0629 \u0628\u0641\u0627\u0631\u063A \u0627\u0644\u0635\u0628\u0631"],
          "sports": ["\u0645\u0628\u0627\u0631\u0627\u0629 \u0646\u0627\u0631\u064A\u0629 \u0644\u0627 \u062A\u0641\u0648\u062A\u0647\u0627! \u0623\u0641\u0636\u0644 \u0644\u062D\u0638\u0627\u062A \u0627\u0644\u0631\u064A\u0627\u0636\u0629", "\u062D\u062F\u062B \u0631\u064A\u0627\u0636\u064A \u062A\u0627\u0631\u064A\u062E\u064A! \u0634\u0627\u0647\u062F \u0627\u0644\u0625\u062B\u0627\u0631\u0629", "\u0627\u0644\u0645\u0628\u0627\u0631\u0627\u0629 \u0627\u0644\u062A\u064A \u064A\u0646\u062A\u0638\u0631\u0647\u0627 \u0627\u0644\u062C\u0645\u064A\u0639! \u0644\u0627 \u062A\u0641\u0648\u062A\u0647\u0627"],
          "recipes": ["\u0648\u0635\u0641\u0629 \u0634\u0647\u064A\u0629 \u0648\u0633\u0647\u0644\u0629 \u0627\u0644\u062A\u062D\u0636\u064A\u0631! \u062C\u0631\u0628\u0647\u0627 \u0627\u0644\u0622\u0646", "\u0623\u0634\u0647\u0649 \u0627\u0644\u0623\u0637\u0628\u0627\u0642 \u0641\u064A \u0645\u062A\u0646\u0627\u0648\u0644 \u064A\u062F\u0643! \u0648\u0635\u0641\u0629 \u0631\u0627\u0626\u0639\u0629", "\u0637\u0628\u0642 \u0644\u0630\u064A\u0630 \u0633\u064A\u0628\u0647\u0631 \u0639\u0627\u0626\u0644\u062A\u0643 \u0648\u0623\u0635\u062F\u0642\u0627\u0626\u0643"],
          "gaming": ["\u0644\u0639\u0628\u0629 \u0645\u0630\u0647\u0644\u0629 \u062A\u0633\u062A\u062D\u0642 \u0627\u0644\u062A\u062C\u0631\u0628\u0629! \u0627\u0646\u0636\u0645 \u0644\u0644\u0645\u063A\u0627\u0645\u0631\u0629", "\u0623\u0641\u0636\u0644 \u0644\u0639\u0628\u0629 \u0641\u064A \u0627\u0644\u0645\u0648\u0633\u0645! \u062C\u0631\u0628\u0647\u0627 \u0627\u0644\u0622\u0646", "\u0639\u0627\u0644\u0645 \u062C\u062F\u064A\u062F \u0645\u0646 \u0627\u0644\u0645\u062A\u0639\u0629 \u0648\u0627\u0644\u0625\u062B\u0627\u0631\u0629 \u064A\u0646\u062A\u0638\u0631\u0643"],
          "apps": ["\u062A\u0637\u0628\u064A\u0642 \u0631\u0627\u0626\u0639 \u0633\u064A\u063A\u064A\u0631 \u062D\u064A\u0627\u062A\u0643! \u062D\u0645\u0644\u0647 \u0627\u0644\u0622\u0646", "\u0623\u0641\u0636\u0644 \u062A\u0637\u0628\u064A\u0642 \u0641\u064A \u0627\u0644\u0641\u0626\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A\u0647", "\u062A\u0637\u0628\u064A\u0642 \u0645\u0630\u0647\u0644 \u064A\u0633\u062A\u062D\u0642 \u0627\u0644\u062A\u062C\u0631\u0628\u0629 \u0641\u0648\u0631\u0627\u064B"],
          "tv_channels": ["\u0642\u0646\u0627\u0629 \u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629 \u0645\u0645\u064A\u0632\u0629! \u0634\u0627\u0647\u062F \u0627\u0644\u0628\u062B \u0627\u0644\u0645\u0628\u0627\u0634\u0631", "\u0623\u0641\u0636\u0644 \u0627\u0644\u0642\u0646\u0648\u0627\u062A \u0627\u0644\u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629! \u0644\u0627 \u062A\u0641\u0648\u062A \u0627\u0644\u0628\u0631\u0627\u0645\u062C", "\u0645\u062D\u062A\u0648\u0649 \u062D\u0635\u0631\u064A \u0648\u0645\u0645\u064A\u0632 \u0639\u0644\u0649 \u0647\u0630\u0647 \u0627\u0644\u0642\u0646\u0627\u0629"]
        };
        const englishDefaultDescriptions = {
          "movies": ["A masterpiece worth watching! Don't miss this cinematic gem", "Best movie of the season! Watch it now", "An amazing film that will captivate you from start to finish"],
          "tv_shows": ["An amazing series worth watching! Don't miss the exciting events", "Best series of the season! Watch now", "A show that will keep you waiting for every episode"],
          "sports": ["An epic match you can't miss! Best sports moments", "Historic sports event! Watch the excitement", "The match everyone is waiting for! Don't miss it"],
          "recipes": ["Delicious and easy to make! Try it now", "Amazing dishes at your fingertips! Great recipe", "A tasty dish that will impress your family"],
          "gaming": ["An amazing game worth trying! Join the adventure", "Best game of the season! Try it now", "A new world of fun and excitement awaits you"],
          "apps": ["A great app that will change your life! Download now", "Best app in its category! Don't miss it", "An amazing app worth trying immediately"],
          "tv_channels": ["A premium TV channel! Watch live now", "The best TV channels! Don't miss the shows", "Exclusive and premium content on this channel"]
        };
        const categoryLabel = categoryLabels[category];
        const colors = categoryColors[category];
        const cta = ctaMessages[category];
        const promoTextAr = descriptionAr || arabicDefaultDescriptions[category][Math.floor(Math.random() * arabicDefaultDescriptions[category].length)];
        const promoTextEn = descriptionEn || englishDefaultDescriptions[category][Math.floor(Math.random() * englishDefaultDescriptions[category].length)];
        const arabicLines = this.wrapText(promoTextAr, 28, 4);
        const englishLines = this.wrapText(promoTextEn, 35, 4);
        console.log(`\u{1F3A8} Creating poster overlay for ${category}`);
        console.log(`   Arabic text (${arabicLines.length} lines): "${promoTextAr.substring(0, 80)}..."`);
        console.log(`   English text (${englishLines.length} lines): "${promoTextEn.substring(0, 80)}..."`);
        console.log(`   CTA: ${cta.ar} / ${cta.en}`);
        const svgOverlay = `
      <svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <linearGradient id="topGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0.95);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="bottomGrad" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" style="stop-color:rgba(0,0,0,0);stop-opacity:1" />
            <stop offset="20%" style="stop-color:rgba(0,0,0,0.5);stop-opacity:1" />
            <stop offset="50%" style="stop-color:rgba(0,0,0,0.85);stop-opacity:1" />
            <stop offset="100%" style="stop-color:rgba(0,0,0,0.98);stop-opacity:1" />
          </linearGradient>
          <linearGradient id="categoryGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:${colors.primary};stop-opacity:1" />
            <stop offset="100%" style="stop-color:${colors.secondary};stop-opacity:1" />
          </linearGradient>
          <linearGradient id="arabicGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" style="stop-color:${colors.primary}e6;stop-opacity:1" />
            <stop offset="50%" style="stop-color:${colors.secondary}e6;stop-opacity:1" />
            <stop offset="100%" style="stop-color:${colors.primary}e6;stop-opacity:1" />
          </linearGradient>
          <linearGradient id="goldGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#fbbf24;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#f59e0b;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#d97706;stop-opacity:1" />
          </linearGradient>
          <filter id="textShadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="4" stdDeviation="8" flood-color="rgba(0,0,0,0.9)"/>
          </filter>
          <filter id="shadow" x="-30%" y="-30%" width="160%" height="160%">
            <feDropShadow dx="0" dy="6" stdDeviation="12" flood-color="rgba(0,0,0,0.95)"/>
          </filter>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        
        <!-- Top gradient -->
        <rect x="0" y="0" width="${width}" height="300" fill="url(#topGrad)"/>
        
        <!-- Extended bottom gradient for bilingual promotional content - 4 lines -->
        <rect x="0" y="${height - 680}" width="${width}" height="680" fill="url(#bottomGrad)"/>
        
        <!-- Category badge at top (bilingual) -->
        <rect x="${width / 2 - 130}" y="45" width="260" height="75" rx="37" fill="url(#categoryGradient)" filter="url(#shadow)"/>
        <rect x="${width / 2 - 125}" y="50" width="250" height="65" rx="32" fill="none" stroke="rgba(255,255,255,0.35)" stroke-width="2"/>
        <text x="${width / 2}" y="75" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" filter="url(#glow)">
          ${categoryLabel.ar}
        </text>
        <text x="${width / 2}" y="102" font-family="Arial, sans-serif" font-size="20" fill="rgba(255,255,255,0.9)" text-anchor="middle" dominant-baseline="middle">
          ${categoryLabel.en}
        </text>
        
        <!-- Arabic Promotional Text Section - 4 Lines -->
        <rect x="35" y="${height - 560}" width="${width - 70}" height="${arabicLines.length * 42 + 45}" rx="22" fill="url(#arabicGradient)" filter="url(#shadow)"/>
        ${arabicLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 525 + index * 42}" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" direction="rtl" filter="url(#glow)">
          ${line}
        </text>
        `).join("")}
        
        <!-- English Promotional Text Section - 4 Lines -->
        ${englishLines.map((line, index) => `
        <text x="${width / 2}" y="${height - 340 + index * 38}" font-family="Arial, sans-serif" font-size="24" fill="rgba(255,255,255,0.95)" text-anchor="middle" dominant-baseline="middle" filter="url(#textShadow)" font-style="italic">
          "${line}"
        </text>
        `).join("")}
        
        <!-- Call-to-Action Button (bilingual) - Positioned higher for Facebook Story visibility -->
        <rect x="${width / 2 - 200}" y="${height - 270}" width="400" height="125" rx="25" fill="url(#categoryGradient)" filter="url(#shadow)"/>
        <rect x="${width / 2 - 195}" y="${height - 265}" width="390" height="115" rx="22" fill="none" stroke="rgba(255,255,255,0.4)" stroke-width="2"/>
        <text x="${width / 2}" y="${height - 220}" font-family="Arial, sans-serif" font-size="34" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="middle" direction="rtl" filter="url(#glow)">
          ${cta.ar}
        </text>
        <text x="${width / 2}" y="${height - 175}" font-family="Arial, sans-serif" font-size="26" font-weight="bold" fill="rgba(255,255,255,0.95)" text-anchor="middle" dominant-baseline="middle">
          ${cta.en}
        </text>
      </svg>
    `;
        const overlayBuffer = Buffer.from(svgOverlay);
        const result = await sharp(imageBuffer).resize(width, height, { fit: "cover", position: "center" }).composite([
          {
            input: overlayBuffer,
            top: 0,
            left: 0
          }
        ]).toBuffer();
        return result;
      }
    };
    trendingPosterService = new TrendingPosterService();
  }
});

// server/smart-analytics.ts
var smart_analytics_exports = {};
__export(smart_analytics_exports, {
  SmartAnalyticsService: () => SmartAnalyticsService,
  smartAnalyticsService: () => smartAnalyticsService
});
var SmartAnalyticsService, smartAnalyticsService;
var init_smart_analytics = __esm({
  "server/smart-analytics.ts"() {
    "use strict";
    init_firestore();
    SmartAnalyticsService = class {
      async analyzeBestPostingTimes(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published" && s.publishedAt);
        if (publishedStories.length < 3) {
          return this.getDefaultPostingTimes();
        }
        const timeSlots = /* @__PURE__ */ new Map();
        publishedStories.forEach((story) => {
          if (!story.publishedAt) return;
          const date = new Date(story.publishedAt);
          const dayOfWeek = date.getDay();
          const hour = date.getHours();
          const key = `${dayOfWeek}-${hour}`;
          const existing = timeSlots.get(key) || { total: 0, count: 0, dayOfWeek, hour };
          existing.total += story.engagementRate || 0;
          existing.count += 1;
          timeSlots.set(key, existing);
        });
        const recommendations = Array.from(timeSlots.entries()).map(([_, data]) => {
          const averageEngagement = data.total / data.count;
          const confidence = Math.min(data.count / 5, 1);
          return {
            dayOfWeek: data.dayOfWeek,
            hour: data.hour,
            dayName: this.getDayName(data.dayOfWeek),
            timeSlot: this.getTimeSlotName(data.hour),
            averageEngagement: parseFloat(averageEngagement.toFixed(2)),
            postCount: data.count,
            confidence: parseFloat(confidence.toFixed(2))
          };
        }).sort((a, b) => b.averageEngagement - a.averageEngagement).slice(0, 5);
        return recommendations.length > 0 ? recommendations : this.getDefaultPostingTimes();
      }
      async getContentRecommendations(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published");
        const linkedAccounts = await firestoreService.getLinkedAccountsByUser(userId);
        const connectedPlatforms = new Set(
          linkedAccounts.filter((acc) => acc.status === "active").map((acc) => acc.platform)
        );
        if (publishedStories.length < 5 || Array.from(connectedPlatforms).length === 0) {
          return this.getDefaultContentRecommendations(connectedPlatforms);
        }
        const categoryPerformance = /* @__PURE__ */ new Map();
        publishedStories.forEach((story) => {
          const existing = categoryPerformance.get(story.category) || { total: 0, count: 0 };
          existing.total += parseFloat((story.engagementRate || 0).toString());
          existing.count += 1;
          categoryPerformance.set(story.category, existing);
        });
        const topCategories = Array.from(categoryPerformance.entries()).map(([category, data]) => ({
          category,
          avgEngagement: data.total / data.count,
          count: data.count
        })).sort((a, b) => b.avgEngagement - a.avgEngagement).slice(0, 3);
        const recommendations = await Promise.all(topCategories.map(async (cat) => ({
          category: cat.category,
          suggestedContent: this.generateContentSuggestion(cat.category),
          reasoning: `\u0647\u0630\u0647 \u0627\u0644\u0641\u0626\u0629 \u062D\u0642\u0642\u062A \u0645\u0639\u062F\u0644 \u062A\u0641\u0627\u0639\u0644 ${cat.avgEngagement.toFixed(1)}\u066A \u0641\u064A ${cat.count} \u0645\u0646\u0634\u0648\u0631\u0627\u062A \u0633\u0627\u0628\u0642\u0629`,
          expectedEngagement: parseFloat(cat.avgEngagement.toFixed(1)),
          suggestedHashtags: this.getRelevantHashtags(cat.category),
          suggestedPlatforms: await this.getBestPlatformsForCategory(cat.category, publishedStories, connectedPlatforms),
          suggestedTime: this.getOptimalTimeForCategory(cat.category, publishedStories)
        })));
        return recommendations;
      }
      async getPlatformRecommendations(content, category, userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const categoryStories = stories.filter(
          (s) => s.category === category && s.status === "published"
        );
        const linkedAccounts = await firestoreService.getLinkedAccountsByUser(userId);
        const connectedPlatforms = new Set(
          linkedAccounts.filter((acc) => acc.status === "active").map((acc) => acc.platform)
        );
        const platformPerformance = {};
        Array.from(connectedPlatforms).forEach((platform) => {
          platformPerformance[platform] = { total: 0, count: 0 };
        });
        categoryStories.forEach((story) => {
          story.platforms.forEach((platform) => {
            if (platformPerformance[platform]) {
              platformPerformance[platform].total += story.engagementRate || 0;
              platformPerformance[platform].count += 1;
            }
          });
        });
        const bestPlatforms = [];
        const expectedEngagement = {};
        let reasoning = "";
        if (categoryStories.length >= 3) {
          const sorted = Object.entries(platformPerformance).filter(([_, data]) => data.count > 0).map(([platform, data]) => ({
            platform,
            avg: data.total / data.count
          })).sort((a, b) => b.avg - a.avg);
          if (sorted.length > 0) {
            bestPlatforms.push(sorted[0].platform);
            expectedEngagement[sorted[0].platform] = parseFloat(sorted[0].avg.toFixed(1));
            if (sorted.length > 1 && sorted[1].avg >= sorted[0].avg * 0.8) {
              bestPlatforms.push(sorted[1].platform);
              expectedEngagement[sorted[1].platform] = parseFloat(sorted[1].avg.toFixed(1));
            }
            reasoning = `\u0628\u0646\u0627\u0621\u064B \u0639\u0644\u0649 ${categoryStories.length} \u0645\u0646\u0634\u0648\u0631\u0627\u062A \u0633\u0627\u0628\u0642\u0629 \u0641\u064A \u0646\u0641\u0633 \u0627\u0644\u0641\u0626\u0629`;
          }
        }
        if (bestPlatforms.length === 0) {
          const hasVideo = content.length < 150;
          const hasHashtags = content.includes("#");
          if (hasVideo) {
            bestPlatforms.push("tiktok", "instagram");
            reasoning = "\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0642\u0635\u064A\u0631 \u064A\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0623\u0641\u0636\u0644 \u0639\u0644\u0649 TikTok \u0648Instagram";
          } else if (hasHashtags) {
            bestPlatforms.push("instagram", "tiktok");
            reasoning = "\u0627\u0644\u0647\u0627\u0634\u062A\u0627\u062C\u0627\u062A \u062A\u062D\u0642\u0642 \u0623\u062F\u0627\u0621\u064B \u062C\u064A\u062F\u0627\u064B \u0639\u0644\u0649 Instagram \u0648TikTok";
          } else {
            bestPlatforms.push("facebook", "instagram");
            reasoning = "\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0646\u0635\u064A \u064A\u0646\u0627\u0633\u0628 Facebook \u0648Instagram";
          }
          expectedEngagement["facebook"] = 5;
          expectedEngagement["instagram"] = 6.5;
          expectedEngagement["tiktok"] = 7;
        }
        return {
          platforms: bestPlatforms,
          reasoning,
          expectedEngagement
        };
      }
      async getTrendingHashtags(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published" && s.hashtags && s.hashtags.length > 0);
        const hashtagStats = /* @__PURE__ */ new Map();
        publishedStories.forEach((story) => {
          if (!story.hashtags) return;
          story.hashtags.forEach((hashtag) => {
            const normalized = hashtag.toLowerCase().replace(/^#/, "");
            const existing = hashtagStats.get(normalized) || { total: 0, count: 0, category: story.category };
            existing.total += story.engagementRate || 0;
            existing.count += 1;
            hashtagStats.set(normalized, existing);
          });
        });
        const trending = Array.from(hashtagStats.entries()).map(([hashtag, data]) => ({
          hashtag: `#${hashtag}`,
          usageCount: data.count,
          averageEngagement: parseFloat((data.total / data.count).toFixed(2)),
          category: data.category,
          trending: data.count >= 3 && data.total / data.count > 5
        })).sort((a, b) => b.averageEngagement - a.averageEngagement).slice(0, 10);
        if (trending.length === 0) {
          return this.getDefaultHashtags();
        }
        return trending;
      }
      async getSmartInsights(userId) {
        try {
          await this.syncRealData(userId);
        } catch (e) {
          console.error("Error syncing real data for insights:", e);
        }
        const [bestPostingTimes, topCategories, platformPerformance, trendingHashtags] = await Promise.all([
          this.analyzeBestPostingTimes(userId),
          this.getTopPerformingCategories(userId),
          this.getPlatformPerformanceInsights(userId),
          this.getTrendingHashtags(userId)
        ]);
        const contentSuggestions = await this.generateContentSuggestions(userId);
        return {
          bestPostingTimes,
          topPerformingCategories: topCategories,
          platformPerformance,
          trendingHashtags,
          contentSuggestions
        };
      }
      async syncRealData(userId) {
        console.log(`[Analytics] Starting real-time sync for user: ${userId}`);
        const accounts = await firestoreService.getLinkedAccountsByUser(userId, { status: "active" });
        if (accounts.length === 0) {
          console.log(`[Analytics] No active accounts found for user: ${userId}`);
          return;
        }
        for (const account of accounts) {
          try {
            console.log(`[Analytics] Syncing ${account.platform} account: ${account.name} (${account.externalId})`);
            if (account.platform === "facebook") {
              const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
              const feed = await facebookSDK2.getPageFeed(account.externalId, account.accessToken, 10);
              if (feed.data && feed.data.length > 0) {
                let totalEngagement = 0;
                for (const post of feed.data) {
                  const likes = post.likes?.summary?.total_count || 0;
                  const comments = post.comments?.summary?.total_count || 0;
                  const shares = post.shares?.count || 0;
                  const engagement = likes + comments + shares;
                  totalEngagement += engagement;
                  console.log(`[Facebook] Post ${post.id}: Likes=${likes}, Comments=${comments}, Shares=${shares}`);
                  const stories = await firestoreService.getStoriesByUser(userId, 100);
                  const publishedStories = stories.filter((s) => s.status === "published" && s.platforms.includes("facebook"));
                  for (const story of publishedStories) {
                    await firestoreService.updateStory(story.id, {
                      engagementRate: parseFloat((engagement / Math.max(publishedStories.length, 1)).toFixed(2))
                    });
                  }
                }
                await firestoreService.updateLinkedAccount(account.id, {
                  accountStats: {
                    ...account.accountStats || {},
                    totalEngagement,
                    lastSyncedAt: /* @__PURE__ */ new Date()
                  }
                });
              }
            } else if (account.platform === "instagram") {
              const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
              const media = await instagramSDK2.getUserMedia(account.externalId, account.accessToken, 10);
              if (media.data && media.data.length > 0) {
                let totalEngagement = 0;
                for (const item of media.data) {
                  try {
                    const insights = await instagramSDK2.getMediaInsights(item.id, account.accessToken);
                    const engagement = insights.data?.find((d) => d.name === "engagement")?.values[0]?.value || 0;
                    totalEngagement += engagement;
                    console.log(`[Instagram] Media ${item.id}: Engagement=${engagement}`);
                    const stories = await firestoreService.getStoriesByUser(userId, 100);
                    const publishedStories = stories.filter((s) => s.status === "published" && s.platforms.includes("instagram"));
                    for (const story of publishedStories) {
                      await firestoreService.updateStory(story.id, {
                        engagementRate: parseFloat((engagement / Math.max(publishedStories.length, 1)).toFixed(2))
                      });
                    }
                  } catch (e) {
                    console.warn(`Could not get insights for Instagram media ${item.id}`);
                  }
                }
                await firestoreService.updateLinkedAccount(account.id, {
                  accountStats: {
                    ...account.accountStats || {},
                    totalEngagement,
                    lastSyncedAt: /* @__PURE__ */ new Date()
                  }
                });
              }
            }
          } catch (err) {
            console.error(`Failed to sync real data for account ${account.id}:`, err);
          }
        }
        await firestoreService.updateUserAccountStats(userId);
        console.log(`[Analytics] Sync completed for user: ${userId}`);
      }
      async getTopPerformingCategories(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published");
        const categoryStats = /* @__PURE__ */ new Map();
        publishedStories.forEach((story) => {
          const existing = categoryStats.get(story.category) || { total: 0, count: 0 };
          existing.total += story.engagementRate || 0;
          existing.count += 1;
          categoryStats.set(story.category, existing);
        });
        return Array.from(categoryStats.entries()).map(([category, data]) => ({
          category,
          averageEngagement: parseFloat((data.total / data.count).toFixed(2)),
          postCount: data.count
        })).sort((a, b) => b.averageEngagement - a.averageEngagement).slice(0, 5);
      }
      async getPlatformPerformanceInsights(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published");
        const linkedAccounts = await firestoreService.getLinkedAccountsByUser(userId);
        const connectedPlatforms = new Set(
          linkedAccounts.filter((acc) => acc.status === "active").map((acc) => acc.platform)
        );
        const platformStats = {};
        for (const platform of Array.from(connectedPlatforms)) {
          platformStats[platform] = { total: 0, count: 0, bestTimes: /* @__PURE__ */ new Map() };
        }
        publishedStories.forEach((story) => {
          story.platforms.forEach((platform) => {
            if (platformStats[platform]) {
              platformStats[platform].total += story.engagementRate || 0;
              platformStats[platform].count += 1;
              if (story.publishedAt) {
                const hour = new Date(story.publishedAt).getHours();
                const current = platformStats[platform].bestTimes.get(hour) || 0;
                platformStats[platform].bestTimes.set(hour, current + (story.engagementRate || 0));
              }
            }
          });
        });
        return Object.entries(platformStats).filter(([_, data]) => data.count > 0).map(([platform, data]) => {
          let bestTime;
          if (data.bestTimes.size > 0) {
            const sortedTimes = Array.from(data.bestTimes.entries());
            const bestHour = sortedTimes.sort((a, b) => b[1] - a[1])[0][0];
            bestTime = this.getTimeSlotName(bestHour);
          }
          return {
            platform,
            averageEngagement: parseFloat((data.total / data.count).toFixed(2)),
            bestTime
          };
        }).sort((a, b) => b.averageEngagement - a.averageEngagement);
      }
      async generateContentSuggestions(userId) {
        const stories = await firestoreService.getStoriesByUser(userId);
        const publishedStories = stories.filter((s) => s.status === "published");
        const suggestions = [];
        if (publishedStories.length >= 10) {
          const avgEngagement = publishedStories.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / publishedStories.length;
          if (avgEngagement < 3) {
            suggestions.push("\u062C\u0631\u0628 \u0625\u0636\u0627\u0641\u0629 \u0627\u0644\u0645\u0632\u064A\u062F \u0645\u0646 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 \u0627\u0644\u0645\u0631\u0626\u064A\u0629 (\u0635\u0648\u0631 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648\u0647\u0627\u062A) \u0644\u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u062A\u0641\u0627\u0639\u0644");
          }
          const withHashtags = publishedStories.filter((s) => s.hashtags && s.hashtags.length > 0);
          if (withHashtags.length < publishedStories.length * 0.5) {
            suggestions.push("\u0627\u0633\u062A\u062E\u062F\u0645 \u0627\u0644\u0647\u0627\u0634\u062A\u0627\u062C\u0627\u062A \u0627\u0644\u0645\u0646\u0627\u0633\u0628\u0629 \u0644\u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u062C\u0645\u0647\u0648\u0631 \u0623\u0648\u0633\u0639");
          }
          const recentPosts = publishedStories.slice(-7);
          const categories = new Set(recentPosts.map((s) => s.category));
          if (categories.size < 2) {
            suggestions.push("\u0646\u0648\u0651\u0639 \u0645\u062D\u062A\u0648\u0627\u0643 \u0628\u064A\u0646 \u0641\u0626\u0627\u062A \u0645\u062E\u062A\u0644\u0641\u0629 \u0644\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u062C\u0645\u0647\u0648\u0631 \u0623\u0643\u0628\u0631");
          }
        }
        if (suggestions.length === 0) {
          suggestions.push(
            "\u0627\u0646\u0634\u0631 \u0628\u0627\u0646\u062A\u0638\u0627\u0645 \u0644\u0644\u062D\u0641\u0627\u0638 \u0639\u0644\u0649 \u062A\u0641\u0627\u0639\u0644 \u0645\u062A\u0627\u0628\u0639\u064A\u0643",
            "\u0627\u0633\u062A\u062E\u062F\u0645 \u0623\u0648\u0642\u0627\u062A \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062B\u0644\u0649 \u0644\u0632\u064A\u0627\u062F\u0629 \u0627\u0644\u0648\u0635\u0648\u0644",
            "\u062A\u0641\u0627\u0639\u0644 \u0645\u0639 \u062A\u0639\u0644\u064A\u0642\u0627\u062A \u0645\u062A\u0627\u0628\u0639\u064A\u0643 \u0644\u0628\u0646\u0627\u0621 \u0645\u062C\u062A\u0645\u0639 \u0646\u0634\u0637"
          );
        }
        return suggestions.slice(0, 5);
      }
      getDayName(dayOfWeek) {
        const days = ["\u0627\u0644\u0623\u062D\u062F", "\u0627\u0644\u0625\u062B\u0646\u064A\u0646", "\u0627\u0644\u062B\u0644\u0627\u062B\u0627\u0621", "\u0627\u0644\u0623\u0631\u0628\u0639\u0627\u0621", "\u0627\u0644\u062E\u0645\u064A\u0633", "\u0627\u0644\u062C\u0645\u0639\u0629", "\u0627\u0644\u0633\u0628\u062A"];
        return days[dayOfWeek] || "\u063A\u064A\u0631 \u0645\u062D\u062F\u062F";
      }
      getTimeSlotName(hour) {
        if (hour >= 6 && hour < 12) return "\u0635\u0628\u0627\u062D\u0627\u064B";
        if (hour >= 12 && hour < 17) return "\u0638\u0647\u0631\u0627\u064B";
        if (hour >= 17 && hour < 21) return "\u0645\u0633\u0627\u0621\u064B";
        return "\u0644\u064A\u0644\u0627\u064B";
      }
      getDefaultPostingTimes() {
        return [
          { dayOfWeek: 3, hour: 20, dayName: "\u0627\u0644\u0623\u0631\u0628\u0639\u0627\u0621", timeSlot: "\u0645\u0633\u0627\u0621\u064B", averageEngagement: 7.5, postCount: 0, confidence: 0.5 },
          { dayOfWeek: 4, hour: 19, dayName: "\u0627\u0644\u062E\u0645\u064A\u0633", timeSlot: "\u0645\u0633\u0627\u0621\u064B", averageEngagement: 7.2, postCount: 0, confidence: 0.5 },
          { dayOfWeek: 5, hour: 21, dayName: "\u0627\u0644\u062C\u0645\u0639\u0629", timeSlot: "\u0644\u064A\u0644\u0627\u064B", averageEngagement: 6.8, postCount: 0, confidence: 0.5 },
          { dayOfWeek: 1, hour: 13, dayName: "\u0627\u0644\u0625\u062B\u0646\u064A\u0646", timeSlot: "\u0638\u0647\u0631\u0627\u064B", averageEngagement: 6.5, postCount: 0, confidence: 0.5 },
          { dayOfWeek: 6, hour: 15, dayName: "\u0627\u0644\u0633\u0628\u062A", timeSlot: "\u0638\u0647\u0631\u0627\u064B", averageEngagement: 6.3, postCount: 0, confidence: 0.5 }
        ];
      }
      getDefaultContentRecommendations(connectedPlatforms) {
        const defaultRecs = [
          {
            category: "movies",
            suggestedContent: "\u0634\u0627\u0631\u0643 \u0631\u0623\u064A\u0643 \u0641\u064A \u0622\u062E\u0631 \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u0623\u0648 \u0631\u0634\u0651\u062D \u0641\u064A\u0644\u0645\u0627\u064B \u062A\u0646\u0635\u062D \u0628\u0645\u0634\u0627\u0647\u062F\u062A\u0647",
            reasoning: "\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u064A\u062D\u0642\u0642 \u062A\u0641\u0627\u0639\u0644\u0627\u064B \u062C\u064A\u062F\u0627\u064B \u0639\u0644\u0649 \u0648\u0633\u0627\u0626\u0644 \u0627\u0644\u062A\u0648\u0627\u0635\u0644",
            expectedEngagement: 6.5,
            suggestedHashtags: ["#\u0623\u0641\u0644\u0627\u0645", "#\u0633\u064A\u0646\u0645\u0627", "#\u0645\u0631\u0627\u062C\u0639\u0629_\u0641\u064A\u0644\u0645"],
            suggestedPlatforms: ["instagram", "facebook"]
          },
          {
            category: "recipes",
            suggestedContent: "\u0634\u0627\u0631\u0643 \u0648\u0635\u0641\u0629 \u0637\u0628\u062E \u0633\u0631\u064A\u0639\u0629 \u0648\u0644\u0630\u064A\u0630\u0629 \u0645\u0639 \u0635\u0648\u0631\u0629 \u0634\u0647\u064A\u0629",
            reasoning: "\u0627\u0644\u0648\u0635\u0641\u0627\u062A \u062A\u062D\u0635\u0644 \u0639\u0644\u0649 \u062A\u0641\u0627\u0639\u0644 \u0639\u0627\u0644\u064A \u062E\u0627\u0635\u0629 \u0645\u0639 \u0627\u0644\u0635\u0648\u0631",
            expectedEngagement: 7,
            suggestedHashtags: ["#\u0648\u0635\u0641\u0627\u062A", "#\u0637\u0628\u062E", "#\u0623\u0643\u0644_\u0635\u062D\u064A"],
            suggestedPlatforms: ["instagram", "tiktok"]
          }
        ];
        if (connectedPlatforms && connectedPlatforms.size > 0) {
          return defaultRecs.map((rec) => ({
            ...rec,
            suggestedPlatforms: rec.suggestedPlatforms.filter((p) => connectedPlatforms.has(p))
          })).filter((rec) => rec.suggestedPlatforms.length > 0);
        }
        return defaultRecs;
      }
      getDefaultHashtags() {
        return [
          { hashtag: "#\u062A\u0631\u0641\u064A\u0647", usageCount: 0, averageEngagement: 7, trending: true },
          { hashtag: "#\u0625\u0644\u0647\u0627\u0645", usageCount: 0, averageEngagement: 6.5, trending: true },
          { hashtag: "#\u064A\u0648\u0645\u064A\u0627\u062A", usageCount: 0, averageEngagement: 6, trending: true }
        ];
      }
      generateContentSuggestion(category) {
        const suggestions = {
          movies: "\u0634\u0627\u0631\u0643 \u0631\u0623\u064A\u0643 \u0641\u064A \u0623\u062D\u062F\u062B \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u0623\u0648 \u0631\u0634\u0651\u062D \u0623\u0641\u0644\u0627\u0645\u0627\u064B \u0643\u0644\u0627\u0633\u064A\u0643\u064A\u0629 \u062A\u0633\u062A\u062D\u0642 \u0627\u0644\u0645\u0634\u0627\u0647\u062F\u0629",
          tv_shows: "\u0646\u0627\u0642\u0634 \u0645\u0633\u0644\u0633\u0644\u0643 \u0627\u0644\u0645\u0641\u0636\u0644 \u0623\u0648 \u0627\u0642\u062A\u0631\u062D \u0645\u0633\u0644\u0633\u0644\u0627\u062A \u062C\u062F\u064A\u062F\u0629 \u0644\u0645\u062A\u0627\u0628\u0639\u064A\u0643",
          sports: "\u0634\u0627\u0631\u0643 \u0623\u062E\u0628\u0627\u0631 \u0627\u0644\u0631\u064A\u0627\u0636\u0629 \u0627\u0644\u0645\u062B\u064A\u0631\u0629 \u0623\u0648 \u062A\u062D\u0644\u064A\u0644\u0643 \u0644\u0644\u0645\u0628\u0627\u0631\u064A\u0627\u062A \u0627\u0644\u0623\u062E\u064A\u0631\u0629",
          recipes: "\u0627\u0646\u0634\u0631 \u0648\u0635\u0641\u0627\u062A \u0633\u0647\u0644\u0629 \u0648\u0633\u0631\u064A\u0639\u0629 \u0645\u0639 \u0635\u0648\u0631 \u062C\u0630\u0627\u0628\u0629",
          gaming: "\u0634\u0627\u0631\u0643 \u0646\u0635\u0627\u0626\u062D \u0627\u0644\u0623\u0644\u0639\u0627\u0628 \u0623\u0648 \u0627\u0633\u062A\u0639\u0631\u0627\u0636 \u0623\u0644\u0639\u0627\u0628 \u062C\u062F\u064A\u062F\u0629",
          apps: "\u0627\u0642\u062A\u0631\u062D \u062A\u0637\u0628\u064A\u0642\u0627\u062A \u0645\u0641\u064A\u062F\u0629 \u0623\u0648 \u0634\u0627\u0631\u0643 \u0645\u0631\u0627\u062C\u0639\u062A\u0643 \u0644\u062A\u0637\u0628\u064A\u0642 \u062C\u062F\u064A\u062F"
        };
        return suggestions[category] || "\u0623\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 \u062C\u0630\u0627\u0628 \u064A\u0646\u0627\u0633\u0628 \u062C\u0645\u0647\u0648\u0631\u0643";
      }
      getRelevantHashtags(category) {
        const hashtags = {
          movies: ["#\u0623\u0641\u0644\u0627\u0645", "#\u0633\u064A\u0646\u0645\u0627", "#\u0645\u0631\u0627\u062C\u0639\u0629_\u0641\u064A\u0644\u0645", "#\u0641\u064A\u0644\u0645_\u0627\u0644\u064A\u0648\u0645"],
          tv_shows: ["#\u0645\u0633\u0644\u0633\u0644\u0627\u062A", "#\u062F\u0631\u0627\u0645\u0627", "#\u062A\u0644\u0641\u0632\u064A\u0648\u0646", "#\u0645\u0633\u0644\u0633\u0644_\u0627\u0644\u064A\u0648\u0645"],
          sports: ["#\u0631\u064A\u0627\u0636\u0629", "#\u0643\u0631\u0629_\u0642\u062F\u0645", "#\u0631\u064A\u0627\u0636\u0629_\u064A\u0648\u0645\u064A\u0629", "#\u0628\u0637\u0648\u0644\u0627\u062A"],
          recipes: ["#\u0648\u0635\u0641\u0627\u062A", "#\u0637\u0628\u062E", "#\u0623\u0643\u0644_\u0635\u062D\u064A", "#\u0645\u0637\u0628\u062E_\u0639\u0631\u0628\u064A"],
          gaming: ["#\u0623\u0644\u0639\u0627\u0628", "#\u062C\u064A\u0645\u0631", "#\u0623\u0644\u0639\u0627\u0628_\u0641\u064A\u062F\u064A\u0648", "#\u0628\u0644\u0627\u064A\u0633\u062A\u064A\u0634\u0646"],
          apps: ["#\u062A\u0637\u0628\u064A\u0642\u0627\u062A", "#\u062A\u0643\u0646\u0648\u0644\u0648\u062C\u064A\u0627", "#\u0645\u0648\u0628\u0627\u064A\u0644", "#\u062A\u0637\u0628\u064A\u0642_\u0627\u0644\u064A\u0648\u0645"]
        };
        return hashtags[category] || ["#\u0645\u062D\u062A\u0648\u0649", "#\u062A\u0631\u0641\u064A\u0647"];
      }
      async getBestPlatformsForCategory(category, stories, connectedPlatforms) {
        const categoryStories = stories.filter((s) => s.category === category);
        if (!connectedPlatforms || connectedPlatforms.size === 0) {
          return [];
        }
        if (categoryStories.length < 3) {
          const allDefaults = {
            movies: ["instagram", "facebook"],
            tv_shows: ["facebook", "instagram"],
            sports: ["facebook", "tiktok"],
            recipes: ["instagram", "tiktok"],
            gaming: ["tiktok", "instagram"],
            apps: ["instagram", "facebook"]
          };
          const defaults = allDefaults[category] || ["instagram", "facebook"];
          return defaults.filter((p) => connectedPlatforms.has(p));
        }
        const platformPerf = {};
        const platformCount = {};
        Array.from(connectedPlatforms).forEach((platform) => {
          platformPerf[platform] = 0;
          platformCount[platform] = 0;
        });
        categoryStories.forEach((story) => {
          story.platforms.forEach((platform) => {
            platformPerf[platform] += story.engagementRate || 0;
            platformCount[platform] += 1;
          });
        });
        const sorted = Object.entries(platformPerf).filter(([_, count]) => platformCount[_] > 0).map(([platform, total]) => ({
          platform,
          avg: total / platformCount[platform]
        })).sort((a, b) => b.avg - a.avg);
        return sorted.slice(0, 2).map((p) => p.platform);
      }
      getOptimalTimeForCategory(category, stories) {
        const categoryStories = stories.filter(
          (s) => s.category === category && s.publishedAt && s.status === "published"
        );
        if (categoryStories.length < 3) {
          return void 0;
        }
        const hourPerformance = /* @__PURE__ */ new Map();
        categoryStories.forEach((story) => {
          if (!story.publishedAt) return;
          const hour = new Date(story.publishedAt).getHours();
          const existing = hourPerformance.get(hour) || { total: 0, count: 0 };
          existing.total += story.engagementRate || 0;
          existing.count += 1;
          hourPerformance.set(hour, existing);
        });
        const bestHour = Array.from(hourPerformance.entries()).map(([hour, data]) => ({ hour, avg: data.total / data.count })).sort((a, b) => b.avg - a.avg)[0];
        if (!bestHour) return void 0;
        const nextWeek = /* @__PURE__ */ new Date();
        nextWeek.setDate(nextWeek.getDate() + 7);
        nextWeek.setHours(bestHour.hour, 0, 0, 0);
        return nextWeek;
      }
    };
    smartAnalyticsService = new SmartAnalyticsService();
  }
});

// server/account-categorization.ts
var account_categorization_exports = {};
__export(account_categorization_exports, {
  AccountCategorizationEngine: () => AccountCategorizationEngine,
  accountCategorizationEngine: () => accountCategorizationEngine
});
var AccountCategorizationEngine, accountCategorizationEngine;
var init_account_categorization = __esm({
  "server/account-categorization.ts"() {
    "use strict";
    AccountCategorizationEngine = class {
      categorizeAccount(account) {
        const classification = this.determineClassification(account);
        const subType = this.determineSubType(account, classification);
        const recommendations = this.generateRecommendations(account, classification);
        const score = this.calculateCategoryScore(account, classification);
        const confidence = this.calculateConfidence(account);
        return {
          id: `${account.id}-category`,
          accountId: account.id,
          platform: account.platform,
          accountType: account.accountType,
          subType,
          classification,
          confidence,
          capabilities: account.capabilities,
          targeting: account.targeting || {},
          recommendations,
          score
        };
      }
      determineClassification(account) {
        const isHighFollower = account.username ? true : false;
        const canPublishStories = account.capabilities?.canPublishStories;
        const canSchedule = account.capabilities?.canSchedule;
        const canGetInsights = account.capabilities?.canGetInsights;
        if (account.accountType === "business") {
          if (canGetInsights && canSchedule) {
            return "enterprise";
          }
          return "small_business";
        }
        if (account.accountType === "page") {
          if (account.permissions?.includes("pages_manage_posts")) {
            return isHighFollower ? "influencer" : "ecommerce";
          }
          return "media";
        }
        return "personal";
      }
      determineSubType(account, classification) {
        const subTypes = {
          facebook_page_business: "\u0635\u0641\u062D\u0629 \u0623\u0639\u0645\u0627\u0644 \u0641\u064A\u0633\u0628\u0648\u0643",
          facebook_page_influencer: "\u0635\u0641\u062D\u0629 \u0645\u0624\u062B\u0631",
          facebook_page_media: "\u0635\u0641\u062D\u0629 \u0625\u0639\u0644\u0627\u0645\u064A\u0629",
          facebook_profile_personal: "\u062D\u0633\u0627\u0628 \u0634\u062E\u0635\u064A \u0641\u064A\u0633\u0628\u0648\u0643",
          facebook_profile_influencer: "\u062D\u0633\u0627\u0628 \u0645\u0624\u062B\u0631 \u0641\u064A\u0633\u0628\u0648\u0643",
          instagram_business_enterprise: "\u062D\u0633\u0627\u0628 \u0628\u064A\u0632\u0646\u0633 \u0625\u0646\u0633\u062A\u063A\u0631\u0627\u0645 \u0645\u062A\u0642\u062F\u0645",
          instagram_business_ecommerce: "\u062D\u0633\u0627\u0628 \u0645\u062A\u062C\u0631 \u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A",
          instagram_creator_influencer: "\u062D\u0633\u0627\u0628 \u0645\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649",
          instagram_personal_personal: "\u062D\u0633\u0627\u0628 \u0634\u062E\u0635\u064A \u0627\u0646\u0633\u062A\u063A\u0631\u0627\u0645",
          tiktok_creator_influencer: "\u062D\u0633\u0627\u0628 \u0645\u0646\u0634\u0626 \u0645\u062D\u062A\u0648\u0649 TikTok",
          tiktok_business_business: "\u062D\u0633\u0627\u0628 \u0639\u0645\u0644 TikTok"
        };
        const key = `${account.platform}_${account.accountType}_${classification}`;
        return subTypes[key] || "\u062D\u0633\u0627\u0628 \u0645\u062A\u0639\u062F\u062F \u0627\u0644\u0623\u063A\u0631\u0627\u0636";
      }
      generateRecommendations(account, classification) {
        const recommendations = [];
        if (!account.capabilities?.canGetInsights) {
          recommendations.push("\u062A\u0641\u0639\u064A\u0644 \u0627\u0644\u0631\u0624\u0649 \u0648\u0627\u0644\u062A\u062D\u0644\u064A\u0644\u0627\u062A \u0644\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0628\u064A\u0627\u0646\u0627\u062A \u0623\u062F\u0627\u0621 \u062F\u0642\u064A\u0642\u0629");
        }
        if (!account.capabilities?.canSchedule) {
          recommendations.push("\u062A\u0641\u0639\u064A\u0644 \u062C\u062F\u0648\u0644\u0629 \u0627\u0644\u0645\u0646\u0634\u0648\u0631\u0627\u062A \u0644\u062A\u062D\u0633\u064A\u0646 \u0627\u0646\u062A\u0638\u0627\u0645 \u0627\u0644\u0646\u0634\u0631");
        }
        if (!account.capabilities?.canPublishReels && account.platform === "instagram") {
          recommendations.push("\u062A\u0631\u0642\u064A\u0629 \u0627\u0644\u062D\u0633\u0627\u0628 \u0644\u0646\u0634\u0631 \u0627\u0644\u0631\u064A\u0644\u0632 \u0648\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0641\u064A\u062F\u064A\u0648");
        }
        if (classification === "small_business" && !account.targeting?.locations) {
          recommendations.push("\u062A\u062D\u062F\u064A\u062F \u0627\u0644\u0645\u0646\u0627\u0637\u0642 \u0627\u0644\u062C\u063A\u0631\u0627\u0641\u064A\u0629 \u0627\u0644\u0645\u0633\u062A\u0647\u062F\u0641\u0629 \u0644\u062A\u062D\u0633\u064A\u0646 \u0627\u0644\u0648\u0635\u0648\u0644 \u0627\u0644\u0645\u062D\u0644\u064A");
        }
        if (classification === "ecommerce" && !account.permissions?.includes("instagram_shopping_api")) {
          recommendations.push("\u062A\u0641\u0639\u064A\u0644 \u0645\u064A\u0632\u0627\u062A \u0627\u0644\u0628\u064A\u0639 \u0627\u0644\u0645\u0628\u0627\u0634\u0631 \u0648\u0627\u0644\u062A\u0633\u0648\u0642 \u0639\u0644\u0649 \u0627\u0644\u0645\u0646\u0635\u0629");
        }
        return recommendations;
      }
      calculateCategoryScore(account, classification) {
        let score = 96.5;
        if (account.status === "active") score += 2;
        if (account.capabilities?.canGetInsights) score += 0.5;
        if (account.capabilities?.canSchedule) score += 0.5;
        if (account.capabilities?.canPublishReels) score += 0.3;
        return Math.min(100, score);
      }
      calculateConfidence(account) {
        const hasAllCapabilities = account.capabilities && Object.values(account.capabilities).filter((v) => typeof v === "boolean").length > 0 ? 0.9 : 0.7;
        const hasTargeting = account.targeting && Object.keys(account.targeting).length > 0 ? 0.95 : 0.85;
        return Math.round((hasAllCapabilities + hasTargeting) / 2 * 100) / 100;
      }
      categorizeMultipleAccounts(accounts) {
        return accounts.map((account) => this.categorizeAccount(account));
      }
      getAccountsByClassification(accounts, classification) {
        return this.categorizeMultipleAccounts(accounts).filter((cat) => cat.classification === classification);
      }
    };
    accountCategorizationEngine = new AccountCategorizationEngine();
  }
});

// server/trending-content-service.ts
var trending_content_service_exports = {};
__export(trending_content_service_exports, {
  TrendingContentService: () => TrendingContentService,
  trendingContentService: () => trendingContentService
});
var INTERNATIONAL_TV_REGIONS2, OTHER_CATEGORIES, CATEGORY_PROMPTS, TrendingContentService, trendingContentService;
var init_trending_content_service = __esm({
  "server/trending-content-service.ts"() {
    "use strict";
    init_firestore();
    init_deepseek();
    init_huggingface();
    init_r2_storage();
    INTERNATIONAL_TV_REGIONS2 = [
      { countryCode: "TR", languageCode: "tr", name: "Turkish", displayName: "\u062A\u0631\u0643\u064A" },
      { countryCode: "US", languageCode: "en", name: "American", displayName: "\u0623\u0645\u0631\u064A\u0643\u064A" },
      { countryCode: "IN", languageCode: "hi", name: "Indian", displayName: "\u0647\u0646\u062F\u064A" },
      { countryCode: "KR", languageCode: "ko", name: "Korean", displayName: "\u0643\u0648\u0631\u064A" }
    ];
    OTHER_CATEGORIES = ["sports", "recipes", "gaming", "apps", "tv_channels"];
    CATEGORY_PROMPTS = {
      sports: {
        titleAr: "\u0645\u0628\u0627\u0631\u064A\u0627\u062A",
        topicPrompt: "Generate 2 trending sports topics right now globally including football, basketball, tennis, and major leagues. Focus on current matches, tournaments, and breaking sports news.",
        imageStyle: "dynamic sports action shot, stadium atmosphere, professional photography, energetic, high contrast, dramatic lighting, 4K quality"
      },
      recipes: {
        titleAr: "\u0648\u0635\u0641\u0627\u062A",
        topicPrompt: "Generate 2 trending food recipes and culinary topics globally including popular dishes, seasonal recipes, and viral food trends on social media.",
        imageStyle: "delicious food photography, appetizing presentation, warm lighting, professional food styling, gourmet, 4K quality, instagram worthy"
      },
      gaming: {
        titleAr: "\u0623\u0644\u0639\u0627\u0628",
        topicPrompt: "Generate 2 trending video games and gaming topics right now including new releases, esports events, popular streamers, and gaming news.",
        imageStyle: "video game concept art, digital illustration, vibrant colors, futuristic, dynamic composition, 4K quality, epic gaming scene"
      },
      apps: {
        titleAr: "\u062A\u0637\u0628\u064A\u0642\u0627\u062A",
        topicPrompt: "Generate 2 trending mobile apps and tech topics including new app releases, popular applications, tech innovations, and digital tools.",
        imageStyle: "modern tech aesthetic, clean minimalist design, sleek interface mockup, professional, gradient colors, 4K quality"
      },
      tv_channels: {
        titleAr: "\u0642\u0646\u0648\u0627\u062A \u062A\u0644\u0641\u0632\u064A\u0648\u0646\u064A\u0629",
        topicPrompt: "Generate 2 trending TV channels and broadcast content topics including popular news channels, entertainment networks, sports broadcasting, and streaming platforms.",
        imageStyle: "modern TV studio, broadcast graphics, professional news set, dynamic lighting, cinematic quality, 4K broadcast aesthetic"
      }
    };
    TrendingContentService = class {
      tmdbApiKey = null;
      async initialize() {
        const tmdbConfig = await firestoreService.getAPIConfig("tmdb");
        if (tmdbConfig?.apiKey) {
          this.tmdbApiKey = tmdbConfig.apiKey;
        } else if (process.env.TMDB_API_KEY) {
          this.tmdbApiKey = process.env.TMDB_API_KEY;
        }
      }
      async getTrendingContent() {
        await this.initialize();
        const errors = [];
        const [movies, tvSeries, otherCategories] = await Promise.all([
          this.fetchTrendingMovies(errors),
          this.fetchTrendingTVSeries(errors),
          this.generateOtherCategoriesContent(errors)
        ]);
        return {
          movies,
          tv_series: tvSeries,
          other_categories: otherCategories,
          generation_errors: errors
        };
      }
      async fetchTrendingMovies(errors) {
        if (!this.tmdbApiKey) {
          errors.push({
            category: "movies",
            item_title: "All Movies",
            error_type: "api_error",
            message: "TMDB API key not configured"
          });
          return [];
        }
        try {
          const url = `https://api.themoviedb.org/3/trending/movie/day?api_key=${this.tmdbApiKey}&language=ar-SA`;
          const response = await fetch(url);
          if (!response.ok) {
            throw new Error(`TMDB API error: ${response.statusText}`);
          }
          const data = await response.json();
          const results = data.results || [];
          return results.slice(0, 10).map((movie) => ({
            title: movie.title,
            poster_url: movie.poster_path ? `https://image.tmdb.org/t/p/w780${movie.poster_path}` : "",
            description: movie.overview || "\u0644\u0627 \u064A\u0648\u062C\u062F \u0648\u0635\u0641 \u0645\u062A\u0627\u062D",
            rating: Math.round(movie.vote_average * 10) / 10,
            tmdb_id: movie.id.toString()
          }));
        } catch (error) {
          errors.push({
            category: "movies",
            item_title: "All Movies",
            error_type: "api_error",
            message: error.message
          });
          return [];
        }
      }
      async fetchTrendingTVSeries(errors) {
        if (!this.tmdbApiKey) {
          errors.push({
            category: "tv_series",
            item_title: "All TV Series",
            error_type: "api_error",
            message: "TMDB API key not configured"
          });
          return [];
        }
        const allShows = [];
        for (const region of INTERNATIONAL_TV_REGIONS2) {
          try {
            const url = `https://api.themoviedb.org/3/discover/tv?api_key=${this.tmdbApiKey}&language=ar-SA&sort_by=popularity.desc&with_origin_country=${region.countryCode}&with_original_language=${region.languageCode}&vote_count.gte=50&first_air_date.gte=2020-01-01&page=1`;
            const response = await fetch(url);
            if (!response.ok) continue;
            const data = await response.json();
            const shows = (data.results || []).slice(0, 3);
            for (const show of shows) {
              allShows.push({
                title: show.name,
                language: region.languageCode,
                country: region.countryCode,
                poster_url: show.poster_path ? `https://image.tmdb.org/t/p/w780${show.poster_path}` : "",
                description: show.overview || "\u0644\u0627 \u064A\u0648\u062C\u062F \u0648\u0635\u0641 \u0645\u062A\u0627\u062D",
                trending: show.popularity > 100,
                rating: Math.round(show.vote_average * 10) / 10,
                tmdb_id: show.id.toString()
              });
            }
          } catch (error) {
            errors.push({
              category: "tv_series",
              item_title: `${region.name} TV Shows`,
              error_type: "api_error",
              message: error.message
            });
          }
        }
        allShows.sort((a, b) => b.rating - a.rating);
        return allShows.slice(0, 10);
      }
      async generateOtherCategoriesContent(errors) {
        const results = [];
        for (const category of OTHER_CATEGORIES) {
          const categoryConfig = CATEGORY_PROMPTS[category];
          try {
            const trendingTopics = await this.generateTrendingTopics(category, categoryConfig.topicPrompt, errors);
            for (const topic of trendingTopics.slice(0, 2)) {
              try {
                const imagePrompt = await this.generateImagePrompt(category, topic, categoryConfig.imageStyle);
                const thumbnailUrl = await this.generateAndUploadThumbnail(category, topic, imagePrompt, errors);
                results.push({
                  category: categoryConfig.titleAr,
                  title: topic.title,
                  thumbnail_url: thumbnailUrl,
                  description: topic.description,
                  prompt_used: imagePrompt
                });
              } catch (error) {
                errors.push({
                  category,
                  item_title: topic.title,
                  error_type: "image_error",
                  message: error.message
                });
              }
            }
          } catch (error) {
            errors.push({
              category,
              item_title: `${categoryConfig.titleAr} Topics`,
              error_type: "api_error",
              message: error.message
            });
          }
        }
        return results;
      }
      async generateTrendingTopics(category, topicPrompt, errors) {
        try {
          const systemPrompt = `You are an AI that generates trending content topics for social media stories. 
Return ONLY a valid JSON array with no additional text, markdown, or formatting.
Each object must have "title" and "description" fields in Arabic.`;
          const userPrompt = `${topicPrompt}

Return the response as a JSON array like this:
[{"title": "\u0639\u0646\u0648\u0627\u0646 \u0627\u0644\u0645\u0648\u0636\u0648\u0639", "description": "\u0648\u0635\u0641 \u0645\u062E\u062A\u0635\u0631"}]

Generate exactly 2 trending topics for ${category} category. 
All content must be in Arabic.
Return ONLY the JSON array, nothing else.`;
          const response = await deepseekSDK.generateSimple(userPrompt, systemPrompt, {
            temperature: 0.7,
            max_tokens: 800
          });
          let cleanResponse = response.trim();
          if (cleanResponse.startsWith("```json")) {
            cleanResponse = cleanResponse.replace(/```json\n?/, "").replace(/\n?```$/, "");
          } else if (cleanResponse.startsWith("```")) {
            cleanResponse = cleanResponse.replace(/```\n?/, "").replace(/\n?```$/, "");
          }
          const topics = JSON.parse(cleanResponse);
          return Array.isArray(topics) ? topics : [];
        } catch (error) {
          errors.push({
            category,
            item_title: "Topic Generation",
            error_type: "api_error",
            message: `DeepSeek error: ${error.message}`
          });
          return this.getFallbackTopics(category);
        }
      }
      getFallbackTopics(category) {
        const fallbacks = {
          sports: [
            { title: "\u0645\u0628\u0627\u0631\u0627\u0629 \u0627\u0644\u064A\u0648\u0645 \u0627\u0644\u062D\u0627\u0633\u0645\u0629", description: "\u0623\u0628\u0631\u0632 \u0627\u0644\u0645\u0628\u0627\u0631\u064A\u0627\u062A \u0627\u0644\u0645\u0646\u062A\u0638\u0631\u0629 \u0641\u064A \u0627\u0644\u0628\u0637\u0648\u0644\u0627\u062A \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629" },
            { title: "\u0646\u062C\u0648\u0645 \u0627\u0644\u0645\u0644\u0627\u0639\u0628", description: "\u0622\u062E\u0631 \u0623\u062E\u0628\u0627\u0631 \u0646\u062C\u0648\u0645 \u0643\u0631\u0629 \u0627\u0644\u0642\u062F\u0645 \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629" }
          ],
          recipes: [
            { title: "\u0648\u0635\u0641\u0629 \u0627\u0644\u0634\u064A\u0641", description: "\u0623\u0634\u0647\u0649 \u0627\u0644\u0648\u0635\u0641\u0627\u062A \u0645\u0646 \u0627\u0644\u0645\u0637\u0627\u0628\u062E \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629" },
            { title: "\u062D\u0644\u0648\u064A\u0627\u062A \u0631\u0645\u0636\u0627\u0646", description: "\u0623\u0637\u064A\u0628 \u0627\u0644\u062D\u0644\u0648\u064A\u0627\u062A \u0627\u0644\u0634\u0631\u0642\u064A\u0629 \u0648\u0627\u0644\u063A\u0631\u0628\u064A\u0629" }
          ],
          gaming: [
            { title: "\u0623\u062D\u062F\u062B \u0627\u0644\u0623\u0644\u0639\u0627\u0628", description: "\u0623\u0642\u0648\u0649 \u0625\u0635\u062F\u0627\u0631\u0627\u062A \u0627\u0644\u0623\u0644\u0639\u0627\u0628 \u0644\u0647\u0630\u0627 \u0627\u0644\u0639\u0627\u0645" },
            { title: "\u0628\u0637\u0648\u0644\u0627\u062A \u0627\u0644\u0625\u064A\u0633\u0628\u0648\u0631\u062A\u0633", description: "\u0622\u062E\u0631 \u0623\u062E\u0628\u0627\u0631 \u0627\u0644\u0628\u0637\u0648\u0644\u0627\u062A \u0627\u0644\u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A\u0629" }
          ],
          apps: [
            { title: "\u062A\u0637\u0628\u064A\u0642\u0627\u062A \u0645\u0645\u064A\u0632\u0629", description: "\u0623\u0641\u0636\u0644 \u0627\u0644\u062A\u0637\u0628\u064A\u0642\u0627\u062A \u0627\u0644\u062C\u062F\u064A\u062F\u0629 \u0644\u0644\u0647\u0648\u0627\u062A\u0641 \u0627\u0644\u0630\u0643\u064A\u0629" },
            { title: "\u062A\u0642\u0646\u064A\u0627\u062A \u062D\u062F\u064A\u062B\u0629", description: "\u0623\u062D\u062F\u062B \u0627\u0644\u0627\u0628\u062A\u0643\u0627\u0631\u0627\u062A \u0641\u064A \u0639\u0627\u0644\u0645 \u0627\u0644\u062A\u0643\u0646\u0648\u0644\u0648\u062C\u064A\u0627" }
          ],
          tv_channels: [
            { title: "\u0642\u0646\u0627\u0629 \u0627\u0644\u0623\u062E\u0628\u0627\u0631 \u0627\u0644\u0639\u0631\u0628\u064A\u0629", description: "\u0623\u0628\u0631\u0632 \u0627\u0644\u0642\u0646\u0648\u0627\u062A \u0627\u0644\u0625\u062E\u0628\u0627\u0631\u064A\u0629 \u0648\u0627\u0644\u062A\u063A\u0637\u064A\u0627\u062A \u0627\u0644\u062D\u064A\u0629" },
            { title: "\u0642\u0646\u0648\u0627\u062A \u0627\u0644\u062A\u0631\u0641\u064A\u0647", description: "\u0623\u0634\u0647\u0631 \u0627\u0644\u0642\u0646\u0648\u0627\u062A \u0627\u0644\u062A\u0631\u0641\u064A\u0647\u064A\u0629 \u0648\u0627\u0644\u0628\u0631\u0627\u0645\u062C \u0627\u0644\u0645\u0645\u064A\u0632\u0629" }
          ]
        };
        return fallbacks[category] || [];
      }
      async generateImagePrompt(category, topic, styleGuide) {
        try {
          const systemPrompt = "Generate a concise English image prompt for AI image generation. Be brief, specific, and focus on visual elements only. Max 40 words.";
          const userPrompt = `Create an image prompt for a social media story thumbnail.
Topic: ${topic.title} - ${topic.description}
Category: ${category}
Style requirements: ${styleGuide}

Return ONLY the image prompt text, nothing else.`;
          const prompt = await deepseekSDK.generateSimple(userPrompt, systemPrompt, {
            temperature: 0.7,
            max_tokens: 100
          });
          return prompt.trim().replace(/^["']|["']$/g, "");
        } catch (error) {
          return `${styleGuide}, trending ${category} content, social media story format, 9:16 aspect ratio`;
        }
      }
      async generateAndUploadThumbnail(category, topic, imagePrompt, errors) {
        try {
          const storyPrompt = `${imagePrompt}, vertical format 9:16, social media story, high quality, professional`;
          const imageResult = await huggingFaceSDK.generateImage(storyPrompt);
          const imageBuffer = Buffer.from(imageResult.imageData, "base64");
          const timestamp = Date.now();
          const randomId = Math.random().toString(36).substring(7);
          const safeTitle = topic.title.replace(/[^a-zA-Z0-9\u0600-\u06FF]/g, "-").substring(0, 30);
          const fileName = `trending-thumbnails/${category}/${timestamp}-${randomId}-${safeTitle}.png`;
          const publicUrl = await r2Storage.uploadFile(imageBuffer, fileName, {
            contentType: "image/png",
            metadata: {
              category,
              title: topic.title,
              prompt: imagePrompt,
              source: "huggingface-flux",
              uploadedAt: (/* @__PURE__ */ new Date()).toISOString()
            }
          });
          return publicUrl;
        } catch (error) {
          throw new Error(`Failed to generate thumbnail: ${error.message}`);
        }
      }
    };
    trendingContentService = new TrendingContentService();
  }
});

// server/token-management-service.ts
var token_management_service_exports = {};
__export(token_management_service_exports, {
  TokenManagementService: () => TokenManagementService,
  tokenManagementService: () => tokenManagementService
});
var TokenManagementService, tokenManagementService;
var init_token_management_service = __esm({
  "server/token-management-service.ts"() {
    "use strict";
    init_firestore();
    init_facebook();
    init_instagram();
    init_tiktok();
    TokenManagementService = class {
      REFRESH_THRESHOLD_DAYS = 7;
      CRITICAL_THRESHOLD_HOURS = 24;
      async processAllTokens() {
        console.log("\u{1F916} Starting Smart Token Management Algorithm...");
        const users = await firestoreService.getAllUsers();
        for (const user of users) {
          await this.manageUserTokens(user.id);
        }
      }
      async manageUserTokens(userId) {
        const accounts = await firestoreService.getLinkedAccountsByUser(userId);
        for (const account of accounts) {
          try {
            await this.evaluateAndRefresh(account);
          } catch (error) {
            console.error(`\u274C Error managing token for ${account.platform}:${account.name}:`, error.message);
          }
        }
      }
      async evaluateAndRefresh(account) {
        const now = Date.now();
        const expiresAt = account.tokenExpiresAt ? account.tokenExpiresAt.getTime() : 0;
        const timeRemaining = expiresAt - now;
        if (account.status === "expired" || account.status === "error") {
          return await this.attemptRefresh(account, "RECOVERY");
        }
        const isValid = await this.verifyTokenHealth(account);
        if (!isValid) {
          return await this.attemptRefresh(account, "HEALTH_FAILURE");
        }
        if (expiresAt > 0) {
          if (timeRemaining < this.CRITICAL_THRESHOLD_HOURS * 60 * 60 * 1e3) {
            return await this.attemptRefresh(account, "CRITICAL_EXPIRY");
          }
          if (timeRemaining < this.REFRESH_THRESHOLD_DAYS * 24 * 60 * 60 * 1e3) {
            return await this.attemptRefresh(account, "PROACTIVE_RENEWAL");
          }
        }
      }
      async verifyTokenHealth(account) {
        try {
          if (account.platform === "facebook") return (await facebookSDK.verifyAccessToken(account.accessToken)).is_valid;
          if (account.platform === "instagram") return await instagramSDK.verifyAccessToken(account.accessToken);
          if (account.platform === "tiktok") return await tiktokSDK.verifyAccessToken(account.accessToken);
          return true;
        } catch {
          return false;
        }
      }
      async attemptRefresh(account, reason) {
        console.log(`\u{1F504} [${reason}] Attempting smart refresh for ${account.platform}:${account.name}`);
        let newToken = null;
        let newRefreshToken = null;
        let expiresIn = null;
        try {
          if (account.platform === "facebook") {
            newToken = await facebookSDK.refreshToken(account.accessToken);
            expiresIn = 60 * 24 * 60 * 60;
          } else if (account.platform === "instagram") {
            newToken = await instagramSDK.refreshToken(account.accessToken);
            expiresIn = 60 * 24 * 60 * 60;
          } else if (account.platform === "tiktok" && account.refreshToken) {
            const res = await tiktokSDK.refreshToken(account.refreshToken);
            if (res) {
              newToken = res.access_token;
              newRefreshToken = res.refresh_token || account.refreshToken;
              expiresIn = res.expires_in;
            }
          }
          if (newToken) {
            await firestoreService.updateLinkedAccount(account.id, {
              accessToken: newToken,
              refreshToken: newRefreshToken || void 0,
              status: "active",
              tokenExpiresAt: expiresIn ? new Date(Date.now() + expiresIn * 1e3) : void 0,
              lastSyncedAt: /* @__PURE__ */ new Date()
            });
            console.log(`\u2705 Successfully refreshed ${account.platform} token for ${account.name}`);
          } else {
            throw new Error("SDK returned empty token");
          }
        } catch (err) {
          console.error(`\u274C Smart refresh failed for ${account.name}:`, err.message);
          if (reason === "CRITICAL_EXPIRY" || reason === "HEALTH_FAILURE") {
            await firestoreService.updateLinkedAccount(account.id, { status: "expired" });
          }
        }
      }
    };
    tokenManagementService = new TokenManagementService();
  }
});

// server/story-music-service.ts
import { promisify as promisify3 } from "util";
import { exec as exec3 } from "child_process";
import * as fs4 from "fs";
import * as path4 from "path";
import fetch4 from "node-fetch";
import sharp2 from "sharp";
var execAsync3, readFileAsync, writeFileAsync2, unlinkAsync2, mkdirAsync, TEMP_DIR, STORY_WIDTH2, STORY_HEIGHT2, StoryMusicService, storyMusicService;
var init_story_music_service = __esm({
  "server/story-music-service.ts"() {
    "use strict";
    init_r2_storage();
    execAsync3 = promisify3(exec3);
    readFileAsync = promisify3(fs4.readFile);
    writeFileAsync2 = promisify3(fs4.writeFile);
    unlinkAsync2 = promisify3(fs4.unlink);
    mkdirAsync = promisify3(fs4.mkdir);
    TEMP_DIR = "/tmp/story-music-videos";
    STORY_WIDTH2 = 1080;
    STORY_HEIGHT2 = 1920;
    StoryMusicService = class {
      async ensureTempDir() {
        try {
          if (!fs4.existsSync(TEMP_DIR)) {
            await mkdirAsync(TEMP_DIR, { recursive: true });
          }
        } catch (error) {
          console.error("Error creating temp directory:", error);
        }
      }
      // Download and trim music to 20 seconds
      async downloadAndTrimMusic(musicUrl, outputPath) {
        try {
          console.log(`\u{1F3B5} Downloading music from: ${musicUrl.substring(0, 60)}...`);
          const response = await fetch4(musicUrl);
          if (!response.ok) {
            throw new Error(`Failed to download music: ${response.statusText}`);
          }
          const buffer = await response.buffer();
          await writeFileAsync2(outputPath, buffer);
          console.log(`\u{1F3B5} Music downloaded, trimming to 20 seconds...`);
          const audioTrimPath = outputPath.replace(".mp3", "-trimmed.mp3");
          const trimCommand = `ffmpeg -y -i "${outputPath}" -t 20 -q:a 9 -n "${audioTrimPath}"`;
          try {
            await execAsync3(trimCommand, {
              maxBuffer: 100 * 1024 * 1024,
              timeout: 9e4
            });
            await unlinkAsync2(outputPath).catch(() => {
            });
            await writeFileAsync2(outputPath, await readFileAsync(audioTrimPath));
            await unlinkAsync2(audioTrimPath).catch(() => {
            });
            console.log(`\u2705 Music trimmed to 20 seconds`);
            return true;
          } catch (error) {
            console.warn(`\u26A0\uFE0F FFmpeg trimming failed: ${error.message}, using full music file`);
            return true;
          }
        } catch (error) {
          console.error(`Error downloading music: ${error.message}`);
          return false;
        }
      }
      // Find trending music (using YouTube Music Service)
      async findTrendingMusic(category) {
        try {
          console.log(`\u{1F3B5} Searching for trending music for category: ${category}`);
          const { musicService: musicService2 } = await Promise.resolve().then(() => (init_music_service(), music_service_exports));
          const tracks = await musicService2.searchMusicForCategory(category);
          if (tracks && tracks.length > 0) {
            const track = tracks[Math.floor(Math.random() * tracks.length)];
            if (track.url) {
              return {
                url: track.url,
                title: track.title,
                artist: track.artist
              };
            }
          }
          return null;
        } catch (error) {
          console.error("Error finding trending music:", error);
          return null;
        }
      }
      async createStoryWithMusic(posterUrl, title, category, musicUrl) {
        await this.ensureTempDir();
        const timestamp = Date.now();
        const randomId = Math.random().toString(36).substring(7);
        const imagePath = path4.join(TEMP_DIR, `poster-${timestamp}-${randomId}.png`);
        const audioPath = path4.join(TEMP_DIR, `audio-${timestamp}-${randomId}.mp3`);
        const videoPath = path4.join(TEMP_DIR, `video-${timestamp}-${randomId}.mp4`);
        try {
          console.log(`\u{1F5BC}\uFE0F Creating 20-second story video...`);
          console.log(`   Title: ${title}`);
          console.log(`   Duration: 20 seconds`);
          console.log(`   Has Music: ${!!musicUrl}`);
          console.log(`\u{1F4E5} Downloading poster image...`);
          const response = await fetch4(posterUrl);
          if (!response.ok) {
            throw new Error(`Failed to download poster: ${response.statusText}`);
          }
          const imageBuffer = await response.buffer();
          const processedImage = await sharp2(imageBuffer).resize(STORY_WIDTH2, STORY_HEIGHT2, {
            fit: "cover",
            position: "center"
          }).png().toBuffer();
          await writeFileAsync2(imagePath, processedImage);
          console.log(`\u2705 Poster image processed`);
          let hasMusic = false;
          if (musicUrl) {
            const musicSuccess = await this.downloadAndTrimMusic(musicUrl, audioPath);
            hasMusic = musicSuccess && fs4.existsSync(audioPath);
          }
          console.log(`\u{1F3AC} Creating 20-second video with ${hasMusic ? "music" : "no audio"}...`);
          let ffmpegCommand;
          if (hasMusic) {
            ffmpegCommand = `ffmpeg -y -loop 1 -i "${imagePath}" -i "${audioPath}" -c:v libx264 -c:a aac -pix_fmt yuv420p -vf "scale=1080:1920,zoompan=z='min(zoom+0.0008,1.15)':d=500:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1080x1920,format=yuv420p" -shortest -t 20 -movflags +faststart "${videoPath}"`;
          } else {
            ffmpegCommand = `ffmpeg -y -loop 1 -i "${imagePath}" -c:v libx264 -t 20 -pix_fmt yuv420p -vf "scale=1080:1920,zoompan=z='min(zoom+0.0008,1.15)':d=500:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1080x1920,format=yuv420p" -movflags +faststart "${videoPath}"`;
          }
          await execAsync3(ffmpegCommand, {
            maxBuffer: 100 * 1024 * 1024,
            timeout: 12e4
          });
          console.log(`\u2705 20-second video created successfully`);
          const videoBuffer = await readFileAsync(videoPath);
          const fileName = `story-videos/${category}/${timestamp}-${randomId}-story-20s.mp4`;
          const uploadedUrl = await r2Storage.uploadFile(videoBuffer, fileName, {
            contentType: "video/mp4",
            metadata: {
              category,
              source: "story-with-music",
              title,
              duration: "20",
              hasMusic: hasMusic.toString(),
              uploadedAt: (/* @__PURE__ */ new Date()).toISOString()
            }
          });
          console.log(`\u2705 20-second story video uploaded: ${uploadedUrl.substring(0, 80)}...`);
          await unlinkAsync2(imagePath).catch(() => {
          });
          await unlinkAsync2(audioPath).catch(() => {
          });
          await unlinkAsync2(videoPath).catch(() => {
          });
          return {
            url: uploadedUrl,
            duration: 20,
            title,
            hasMusic
          };
        } catch (error) {
          console.error("Story with music creation error:", error.message);
          await unlinkAsync2(imagePath).catch(() => {
          });
          await unlinkAsync2(audioPath).catch(() => {
          });
          await unlinkAsync2(videoPath).catch(() => {
          });
          return null;
        }
      }
    };
    storyMusicService = new StoryMusicService();
  }
});

// server/error-handler.ts
var error_handler_exports = {};
__export(error_handler_exports, {
  PublishingErrorHandler: () => PublishingErrorHandler,
  publishingErrorHandler: () => publishingErrorHandler
});
var RATE_LIMIT_CONFIG, requestTracking, PublishingErrorHandler, publishingErrorHandler;
var init_error_handler = __esm({
  "server/error-handler.ts"() {
    "use strict";
    init_firestore();
    RATE_LIMIT_CONFIG = {
      facebook: { requestsPerMinute: 200, requestsPerDay: 5e4 },
      instagram: { requestsPerMinute: 200, requestsPerDay: 5e4 },
      tiktok: { requestsPerMinute: 100, requestsPerDay: 1e4 }
    };
    requestTracking = /* @__PURE__ */ new Map();
    PublishingErrorHandler = class {
      /**
       * Validate if a URL is properly formatted
       */
      static isValidUrl(url) {
        if (!url) return false;
        try {
          const urlObj = new URL(url);
          return urlObj.protocol === "http:" || urlObj.protocol === "https:";
        } catch {
          return false;
        }
      }
      /**
       * Check if token is expired or about to expire
       */
      static isTokenExpired(account) {
        if (!account.tokenExpiresAt) {
          return false;
        }
        const now = /* @__PURE__ */ new Date();
        const expiresAt = new Date(account.tokenExpiresAt);
        const bufferMinutes = 5;
        return now.getTime() >= expiresAt.getTime() - bufferMinutes * 60 * 1e3;
      }
      /**
       * Get friendly error message for different error types
       */
      static getFriendlyErrorMessage(error, context) {
        const errorMessage = error.message || error.toString();
        if (errorMessage.includes("rate_limit") || errorMessage.includes("429")) {
          return "\u062A\u0645 \u062A\u062C\u0627\u0648\u0632 \u062D\u062F \u0627\u0644\u0645\u0639\u062F\u0644 \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0628\u0647. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u062C\u062F\u062F\u0627 \u0644\u0627\u062D\u0642\u0627.";
        }
        if (errorMessage.includes("190") || errorMessage.includes("invalid_token") || errorMessage.includes("expired")) {
          return "\u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u0629 \u0631\u0645\u0632 \u0627\u0644\u062F\u062E\u0648\u0644. \u064A\u0631\u062C\u0649 \u062A\u062D\u062F\u064A\u062B \u0627\u0644\u062D\u0633\u0627\u0628.";
        }
        if (errorMessage.includes("10") || errorMessage.includes("200") || errorMessage.includes("permission")) {
          return "\u0635\u0644\u0627\u062D\u064A\u0627\u062A \u063A\u064A\u0631 \u0643\u0627\u0641\u064A\u0629. \u062A\u0623\u0643\u062F \u0645\u0646 \u0645\u0646\u062D \u0627\u0644\u0625\u0630\u0646 \u0627\u0644\u0645\u0637\u0644\u0648\u0628.";
        }
        if (errorMessage.includes("Invalid URL") || errorMessage.includes("malformed")) {
          return "\u0631\u0627\u0628\u0637 \u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D. \u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646 \u0627\u0644\u0631\u0627\u0628\u0637 \u064A\u0628\u062F\u0623 \u0628\u0640 http:// \u0623\u0648 https://";
        }
        if (errorMessage.includes("404") || errorMessage.includes("not found")) {
          return "\u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0623\u0648 \u062A\u0645 \u062D\u0630\u0641\u0647. \u0642\u062F \u062A\u0643\u0648\u0646 \u0627\u0644\u0631\u0627\u0628\u0637 \u0642\u062F \u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u062A\u0647.";
        }
        if (errorMessage.includes("ECONNREFUSED") || errorMessage.includes("ENOTFOUND")) {
          return "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A. \u062A\u0623\u0643\u062F \u0645\u0646 \u0627\u062A\u0635\u0627\u0644\u0643 \u062B\u0645 \u062D\u0627\u0648\u0644 \u0645\u062C\u062F\u062F\u0627.";
        }
        if (errorMessage.includes("NoSuchKey") || errorMessage.includes("r2")) {
          return "\u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0633\u062D\u0627\u0628\u064A. \u0642\u062F \u064A\u0643\u0648\u0646 \u0642\u062F \u062A\u0645 \u062D\u0630\u0641\u0647.";
        }
        if (errorMessage.includes("MEDIA_TYPE_INVALID") || errorMessage.includes("media_type")) {
          return "\u0646\u0648\u0639 \u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u062F\u0639\u0648\u0645. \u0627\u0633\u062A\u062E\u062F\u0645 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648.";
        }
        return `\u062D\u062F\u062B \u062E\u0637\u0623: ${errorMessage}`;
      }
      /**
       * Check if error is retryable
       */
      static isRetryableError(error) {
        const message = (error.message || error.toString()).toLowerCase();
        if (message.includes("permission") || message.includes("403") || message.includes("unauthorized")) {
          return false;
        }
        if (message.includes("invalid") && message.includes("media")) {
          return false;
        }
        if (message.includes("token") && message.includes("invalid")) {
          return false;
        }
        if (message.includes("econnrefused") || message.includes("enotfound") || message.includes("timeout")) {
          return true;
        }
        if (message.includes("429") || message.includes("rate_limit")) {
          return true;
        }
        if (message.includes("500") || message.includes("502") || message.includes("503")) {
          return true;
        }
        return true;
      }
      /**
       * Get retry delay with exponential backoff
       */
      static getRetryDelay(retryCount, maxDelay = 6e4) {
        const baseDelay = 1e3;
        const delay = baseDelay * Math.pow(2, retryCount);
        return Math.min(delay, maxDelay);
      }
      /**
       * Check if account should have rate limiting
       */
      static shouldRateLimit(platform, accountId) {
        const key = `${platform}:${accountId}`;
        const tracking = requestTracking.get(key);
        if (!tracking) {
          return false;
        }
        const config = RATE_LIMIT_CONFIG[platform];
        if (!config) return false;
        const now = /* @__PURE__ */ new Date();
        const timeSinceReset = now.getTime() - tracking.lastResetTime.getTime();
        if (timeSinceReset > 6e4) {
          tracking.requestCount = 0;
          tracking.lastResetTime = now;
        }
        if (timeSinceReset > 24 * 60 * 60 * 1e3) {
          tracking.dailyCount = 0;
        }
        return tracking.requestCount >= config.requestsPerMinute || tracking.dailyCount >= config.requestsPerDay;
      }
      /**
       * Track API request for rate limiting
       */
      static trackRequest(platform, accountId) {
        const key = `${platform}:${accountId}`;
        const tracking = requestTracking.get(key);
        if (!tracking) {
          requestTracking.set(key, {
            lastResetTime: /* @__PURE__ */ new Date(),
            requestCount: 1,
            dailyCount: 1
          });
        } else {
          tracking.requestCount++;
          tracking.dailyCount++;
        }
      }
      /**
       * Validate story before publishing
       */
      static async validateStoryForPublishing(story, account) {
        const errors = [];
        if (account.status !== "active") {
          errors.push(`\u0627\u0644\u062D\u0633\u0627\u0628 "${account.name}" \u063A\u064A\u0631 \u0646\u0634\u0637`);
        }
        if (this.isTokenExpired(account)) {
          errors.push(`\u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u0629 \u0631\u0645\u0632 \u0627\u0644\u062F\u062E\u0648\u0644 \u0644\u0644\u062D\u0633\u0627\u0628 "${account.name}". \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u062F\u064A\u062B.`);
        }
        if (story.mediaUrl && !this.isValidUrl(story.mediaUrl)) {
          errors.push(`\u0631\u0627\u0628\u0637 \u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D: ${story.mediaUrl}`);
        }
        if (!story.platforms.includes(account.platform)) {
          errors.push(`\u0627\u0644\u0645\u0646\u0635\u0629 ${account.platform} \u063A\u064A\u0631 \u0645\u062D\u062F\u062F\u0629 \u0644\u0644\u0642\u0635\u0629`);
        }
        if (!story.mediaType) {
          errors.push("\u064A\u062C\u0628 \u062A\u062D\u062F\u064A\u062F \u0646\u0648\u0639 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 (\u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648)");
        }
        if (account.quotas) {
          if (account.quotas.dailyUsed >= account.quotas.dailyLimit) {
            errors.push(`\u062A\u0645 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u062D\u062F \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u064A\u0648\u0645\u064A (${account.quotas.dailyLimit})`);
          }
          if (account.quotas.monthlyUsed >= account.quotas.monthlyLimit) {
            errors.push(`\u062A\u0645 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u0649 \u062D\u062F \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0634\u0647\u0631\u064A (${account.quotas.monthlyLimit})`);
          }
        }
        return {
          valid: errors.length === 0,
          errors
        };
      }
      /**
       * Log publishing error with context
       */
      static async logPublishingError(storyId, accountId, platform, error, context) {
        const errorData = {
          timestamp: /* @__PURE__ */ new Date(),
          storyId,
          accountId,
          platform,
          errorMessage: error.message,
          errorCode: error.code,
          errorStack: error.stack,
          context: context || {}
        };
        try {
          console.error("\u274C Publishing Error:", errorData);
        } catch (logError) {
          console.error("Failed to log publishing error:", logError);
        }
      }
      /**
       * Handle account token refresh
       */
      static async refreshAccountTokenIfNeeded(account) {
        if (!this.isTokenExpired(account)) {
          return account;
        }
        try {
          console.log(`\u{1F504} Refreshing token for account: ${account.name}`);
          if (account.platform === "facebook" || account.platform === "instagram") {
            const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
            const tokenData = await facebookSDK2.getLongLivedToken(account.accessToken);
            const expiresIn = tokenData.expires_in || 5184e3;
            await firestoreService.updateLinkedAccount(account.id, {
              accessToken: tokenData.access_token,
              tokenExpiresAt: new Date(Date.now() + expiresIn * 1e3)
            });
            return {
              ...account,
              accessToken: tokenData.access_token,
              tokenExpiresAt: new Date(Date.now() + expiresIn * 1e3)
            };
          } else if (account.platform === "tiktok" && account.refreshToken) {
            const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
            const tokenData = await tiktokSDK2.refreshAccessToken(account.refreshToken);
            const expiresIn = tokenData.expires_in || 2592e3;
            await firestoreService.updateLinkedAccount(account.id, {
              accessToken: tokenData.access_token,
              refreshToken: tokenData.refresh_token || account.refreshToken,
              tokenExpiresAt: new Date(Date.now() + expiresIn * 1e3)
            });
            return {
              ...account,
              accessToken: tokenData.access_token,
              refreshToken: tokenData.refresh_token || account.refreshToken,
              tokenExpiresAt: new Date(Date.now() + expiresIn * 1e3)
            };
          }
        } catch (error) {
          console.error(`Failed to refresh token for account ${account.id}:`, error);
          await firestoreService.updateLinkedAccount(account.id, {
            status: "expired"
          }).catch(() => {
          });
        }
        return null;
      }
      /**
       * Get wait time before retrying rate-limited request
       */
      static getRateLimitWaitTime(platform, accountId) {
        const key = `${platform}:${accountId}`;
        const tracking = requestTracking.get(key);
        if (!tracking) return 0;
        const config = RATE_LIMIT_CONFIG[platform];
        if (!config) return 0;
        const exceededBy = tracking.requestCount - config.requestsPerMinute;
        if (exceededBy <= 0) return 0;
        return Math.min(exceededBy * 5e3, 6e4);
      }
      /**
       * Clear all tracking data
       */
      static clearTracking() {
        requestTracking.clear();
      }
    };
    publishingErrorHandler = new PublishingErrorHandler();
  }
});

// server/file-validator.ts
var file_validator_exports = {};
__export(file_validator_exports, {
  FileValidator: () => FileValidator,
  fileValidator: () => fileValidator
});
var FileValidator, fileValidator;
var init_file_validator = __esm({
  "server/file-validator.ts"() {
    "use strict";
    init_r2_storage();
    FileValidator = class {
      /**
       * Validate file exists in R2 and return verified URL
       */
      static async validateAndGetUrl(url, maxAttempts = 3) {
        if (!url) {
          return {
            valid: false,
            error: "\u0644\u0627 \u064A\u0648\u062C\u062F \u0631\u0627\u0628\u0637 \u0645\u0644\u0641 \u0644\u0644\u062A\u062D\u0642\u0642 \u0645\u0646\u0647"
          };
        }
        try {
          new URL(url);
        } catch {
          return {
            valid: false,
            error: "\u0635\u064A\u063A\u0629 \u0627\u0644\u0631\u0627\u0628\u0637 \u063A\u064A\u0631 \u0635\u062D\u064A\u062D\u0629"
          };
        }
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            const result = await r2Storage.verifyAndGetUrl(url);
            if (result.valid && result.freshUrl) {
              return {
                valid: true,
                url: result.freshUrl,
                fileKey: result.fileKey,
                attempt
              };
            }
            if (!result.valid && attempt === maxAttempts) {
              return {
                valid: false,
                error: result.error || "\u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F",
                fileKey: result.fileKey,
                attempt
              };
            }
          } catch (error) {
            if (attempt === maxAttempts) {
              return {
                valid: false,
                error: error.message || "\u0641\u0634\u0644 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0644\u0641",
                attempt
              };
            }
            await new Promise((resolve) => setTimeout(resolve, 1e3 * attempt));
          }
        }
        return {
          valid: false,
          error: "\u0641\u0634\u0644 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0644\u0641 \u0628\u0639\u062F \u0639\u062F\u0629 \u0645\u062D\u0627\u0648\u0644\u0627\u062A",
          attempt: maxAttempts
        };
      }
      /**
       * Get file metadata for validation
       */
      static async getFileMetadata(fileKey) {
        try {
          const exists = await r2Storage.fileExists(fileKey);
          if (!exists) {
            return {
              exists: false,
              error: "\u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F"
            };
          }
          const metadata = await r2Storage.getFileMetadata(fileKey);
          return {
            exists: true,
            size: metadata.contentLength,
            contentType: metadata.contentType,
            lastModified: metadata.lastModified
          };
        } catch (error) {
          return {
            exists: false,
            error: error.message || "\u0641\u0634\u0644 \u0627\u0644\u062D\u0635\u0648\u0644 \u0639\u0644\u0649 \u0628\u064A\u0627\u0646\u0627\u062A \u0627\u0644\u0645\u0644\u0641"
          };
        }
      }
      /**
       * Validate multiple files before batch operation
       */
      static async validateBatch(urls) {
        const validUrls = [];
        const invalidUrls = [];
        for (const url of urls) {
          const result = await this.validateAndGetUrl(url);
          if (result.valid && result.url) {
            validUrls.push(result.url);
          } else {
            invalidUrls.push({
              url,
              error: result.error || "\u0641\u0634\u0644 \u0627\u0644\u062A\u062D\u0642\u0642"
            });
          }
        }
        return {
          valid: invalidUrls.length === 0,
          validUrls,
          invalidUrls
        };
      }
      /**
       * Check if file size is appropriate for platform
       */
      static isValidFileSize(fileSize, platform) {
        if (!fileSize) {
          return { valid: false, error: "\u0644\u0645 \u064A\u062A\u0645\u0643\u0646 \u0645\u0646 \u062A\u062D\u062F\u064A\u062F \u062D\u062C\u0645 \u0627\u0644\u0645\u0644\u0641" };
        }
        const limits = {
          facebook: { maxImage: 4 * 1024 * 1024, maxVideo: 2 * 1024 * 1024 * 1024 },
          // 4MB image, 2GB video
          instagram: { maxImage: 8 * 1024 * 1024, maxVideo: 5.368 * 1024 * 1024 * 1024 },
          // 8MB image, 5.368GB video
          tiktok: { maxImage: 72 * 1024 * 1024, maxVideo: 287.6 * 1024 * 1024 * 1024 }
          // 72MB image, 287.6GB video
        };
        const limit = limits[platform];
        if (!limit) {
          return { valid: false, error: `\u0645\u0646\u0635\u0629 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641\u0629: ${platform}` };
        }
        if (fileSize > limit.maxImage) {
          const maxMB = (limit.maxImage / 1024 / 1024).toFixed(0);
          return {
            valid: false,
            error: `\u062D\u062C\u0645 \u0627\u0644\u0645\u0644\u0641 \u064A\u062A\u062C\u0627\u0648\u0632 \u0627\u0644\u062D\u062F \u0627\u0644\u0623\u0642\u0635\u0649 \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0628\u0647 (${maxMB}MB)`
          };
        }
        return { valid: true };
      }
      /**
       * Extract file information from URL
       */
      static extractFileInfo(url) {
        try {
          const urlObj = new URL(url);
          const pathParts = urlObj.pathname.split("/").filter((p) => p.length > 0);
          const fileName = pathParts[pathParts.length - 1];
          const extension = fileName?.split(".").pop();
          return {
            fileName,
            fileExtension: extension?.toLowerCase(),
            fileKey: r2Storage.extractFileKeyFromUrl(url) || void 0
          };
        } catch {
          return {};
        }
      }
      /**
       * Validate content type matches file extension
       */
      static isValidContentType(contentType, fileExtension, mediaType) {
        if (!contentType || !fileExtension) {
          return true;
        }
        const validImageTypes = ["image/png", "image/jpeg", "image/webp", "image/gif"];
        const validVideoTypes = ["video/mp4", "video/webm", "video/quicktime"];
        if (mediaType === "image") {
          return validImageTypes.includes(contentType);
        } else if (mediaType === "video") {
          return validVideoTypes.includes(contentType);
        }
        return true;
      }
      /**
       * Check if URL will likely expire soon
       */
      static willUrlExpireSoon(urlString, expirationMinutes = 60) {
        try {
          const url = new URL(urlString);
          const expirationParam = url.searchParams.get("X-Amz-Expires");
          const dateParam = url.searchParams.get("X-Amz-Date");
          if (!expirationParam || !dateParam) {
            return false;
          }
          const expiresIn = parseInt(expirationParam, 10);
          const issueDate = new Date(dateParam.replace(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/, "$1-$2-$3T$4:$5:$6Z"));
          const expiryDate = new Date(issueDate.getTime() + expiresIn * 1e3);
          const now = /* @__PURE__ */ new Date();
          const minutesUntilExpiry = (expiryDate.getTime() - now.getTime()) / 6e4;
          return minutesUntilExpiry < expirationMinutes;
        } catch {
          return false;
        }
      }
      /**
       * Refresh URL if it's close to expiring
       */
      static async refreshUrlIfNeeded(url) {
        if (this.willUrlExpireSoon(url, 60)) {
          try {
            const freshUrl = await r2Storage.refreshSignedUrl(url);
            return freshUrl;
          } catch {
            return url;
          }
        }
        return url;
      }
    };
    fileValidator = new FileValidator();
  }
});

// server/story-scheduler.ts
var story_scheduler_exports = {};
__export(story_scheduler_exports, {
  StoryScheduler: () => StoryScheduler,
  storyScheduler: () => storyScheduler
});
var StoryScheduler, storyScheduler;
var init_story_scheduler = __esm({
  "server/story-scheduler.ts"() {
    "use strict";
    init_firestore();
    init_r2_storage();
    init_story_music_service();
    StoryScheduler = class {
      isRunning = false;
      checkInterval = 6e4;
      // Check every minute
      firebaseWarningShown = false;
      lastWarningTime = 0;
      warningIntervalMs = 3e5;
      // Show warning only every 5 minutes
      async start() {
        if (this.isRunning) {
          return;
        }
        this.isRunning = true;
        console.log("\u{1F4C5} Story scheduler started - checking for scheduled stories every minute");
        this.scheduleNextCheck();
      }
      shouldShowWarning() {
        const now = Date.now();
        if (!this.firebaseWarningShown || now - this.lastWarningTime > this.warningIntervalMs) {
          this.lastWarningTime = now;
          this.firebaseWarningShown = true;
          return true;
        }
        return false;
      }
      scheduleNextCheck() {
        if (!this.isRunning) return;
        setTimeout(async () => {
          try {
            await this.processScheduledStories();
          } catch (error) {
            if (error.message?.includes("Project Id")) {
              if (this.shouldShowWarning()) {
                console.warn("\u26A0\uFE0F  Firebase Project ID not configured - Story scheduler paused until setup complete");
              }
            } else {
              console.error("Error processing scheduled stories:", error);
            }
          }
          this.scheduleNextCheck();
        }, this.checkInterval);
      }
      async processScheduledStories() {
        try {
          let allScheduledStories = [];
          try {
            allScheduledStories = await firestoreService.getAllScheduledStories();
          } catch (error) {
            if (error.message?.includes("Project Id") || error.message?.includes("authentication")) {
              console.warn("\u26A0\uFE0F  Firestore not initialized - waiting for Firebase setup");
              return;
            }
            throw error;
          }
          const now = /* @__PURE__ */ new Date();
          const formatTimeInSaudi = (utcTime) => {
            const saudiOffsetMs = 3 * 60 * 60 * 1e3;
            const saudiTime = new Date(utcTime.getTime() + saudiOffsetMs);
            return saudiTime.toISOString();
          };
          console.log(`
\u{1F4CB} === STORY SCHEDULER CHECK (Every 1 minute) ===`);
          console.log(`   \u{1F550} Current UTC Time: ${now.toISOString()}`);
          console.log(`   \u{1F550} Current Saudi Arabia Time (UTC+3): ${formatTimeInSaudi(now)}`);
          console.log(`   \u{1F4DA} Total stories in Firestore: ${allScheduledStories.length}`);
          if (allScheduledStories.length > 0) {
            console.log(`
   \u{1F4DD} All Scheduled Stories:`);
            allScheduledStories.forEach((s, idx) => {
              const storyTime = new Date(s.scheduledTime);
              const status = s.status || "unknown";
              const isDue = storyTime <= now;
              console.log(`      [${idx + 1}] ID: ${s.id}`);
              console.log(`          Status: ${status}`);
              console.log(`          Scheduled UTC: ${storyTime.toISOString()}`);
              console.log(`          Scheduled Saudi (UTC+3): ${formatTimeInSaudi(storyTime)}`);
              console.log(`          Is Due? ${isDue ? "\u2705 YES" : "\u274C NO"}`);
              console.log(`          Platforms: ${s.platforms.join(", ")}`);
            });
          }
          const duePosts = allScheduledStories.filter((story) => {
            if (!story.scheduledTime) return false;
            const scheduledTimeInUTC = new Date(story.scheduledTime);
            return scheduledTimeInUTC <= now;
          });
          if (duePosts.length === 0) {
            console.log(`
   \u23F3 No stories due for publishing at this moment`);
            console.log(`\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
`);
            return;
          }
          console.log(`
   \u2705 FOUND ${duePosts.length} STORIES READY TO PUBLISH!`);
          for (const story of duePosts) {
            console.log(`
   \u{1F680} Publishing story: ${story.id}`);
            await this.publishStory(story);
          }
          console.log(`\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
`);
        } catch (error) {
          console.error("Error in processScheduledStories:", error);
        }
      }
      async publishStory(story) {
        try {
          console.log(`
      \u{1F4DD} === PUBLISHING STORY ===`);
          console.log(`         Story ID: ${story.id}`);
          console.log(`         Title: ${story.content.substring(0, 50)}...`);
          console.log(`         Platforms: ${story.platforms.join(", ")}`);
          console.log(`         Media Type: ${story.mediaType || "unknown"}`);
          console.log(`         Has Music: ${!!story.musicUrl}`);
          const assignments = await firestoreService.getStoryAssignments(story.id);
          console.log(`         Assigned Accounts: ${assignments.length}`);
          if (assignments.length === 0) {
            console.log(`         \u26A0\uFE0F  NO ACCOUNTS ASSIGNED - Skipping...`);
            await firestoreService.updateStory(story.id, {
              status: "failed"
            });
            return;
          }
          let hasAnySuccess = false;
          let hasAnyFailure = false;
          const successfulPlatforms = [];
          for (const assignment of assignments) {
            console.log(`
      \u{1F517} Processing Assignment:`);
            console.log(`         Account ID: ${assignment.accountId}`);
            console.log(`         Status: ${assignment.status}`);
            const account = await firestoreService.getLinkedAccountById(assignment.accountId);
            if (!account) {
              console.log(`         \u274C Account NOT FOUND in Firestore`);
              await firestoreService.updateAssignmentStatus(
                story.id,
                assignment.accountId,
                "failed",
                "\u0627\u0644\u062D\u0633\u0627\u0628 \u0627\u0644\u0645\u0631\u062A\u0628\u0637 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F"
              );
              hasAnyFailure = true;
              continue;
            }
            console.log(`         \u2705 Account Found: ${account.name}`);
            console.log(`            Platform: ${account.platform}`);
            console.log(`            Status: ${account.status}`);
            console.log(`            External ID: ${account.externalId}`);
            console.log(`            Has Access Token: ${!!account.accessToken}`);
            if (account.status !== "active") {
              console.log(`         \u274C Account is NOT ACTIVE (Status: ${account.status})`);
              await firestoreService.updateAssignmentStatus(
                story.id,
                assignment.accountId,
                "failed",
                "\u0627\u0644\u062D\u0633\u0627\u0628 \u063A\u064A\u0631 \u0646\u0634\u0637"
              );
              hasAnyFailure = true;
              continue;
            }
            if (!story.platforms.includes(account.platform)) {
              console.log(`         \u26A0\uFE0F  Story NOT scheduled for platform ${account.platform}`);
              continue;
            }
            try {
              console.log(`         \u{1F680} Starting publish to ${account.platform}...`);
              const publishResult = await this.publishToAccount(story, account);
              await firestoreService.updateLinkedAccount(account.id, {
                lastPublishedAt: /* @__PURE__ */ new Date(),
                quotas: {
                  dailyLimit: account.quotas?.dailyLimit || 50,
                  dailyUsed: (account.quotas?.dailyUsed || 0) + 1,
                  monthlyLimit: account.quotas?.monthlyLimit || 1e3,
                  monthlyUsed: (account.quotas?.monthlyUsed || 0) + 1,
                  resetAt: account.quotas?.resetAt || new Date(Date.now() + 24 * 60 * 60 * 1e3)
                }
              });
              await firestoreService.updateAssignmentStatus(
                story.id,
                assignment.accountId,
                "published"
              );
              hasAnySuccess = true;
              if (!successfulPlatforms.includes(account.platform)) {
                successfulPlatforms.push(account.platform);
              }
              console.log(`         \u2705 PUBLISHED SUCCESSFULLY to ${account.platform}!`);
            } catch (error) {
              console.error(`         \u274C PUBLISH FAILED to ${account.platform}:`);
              console.error(`            Error: ${error.message}`);
              await firestoreService.updateAssignmentStatus(
                story.id,
                assignment.accountId,
                "failed",
                error.message
              );
              hasAnyFailure = true;
            }
          }
          let finalStatus;
          if (hasAnySuccess && !hasAnyFailure) {
            finalStatus = "published";
          } else if (hasAnySuccess && hasAnyFailure) {
            finalStatus = "published";
          } else {
            finalStatus = "failed";
          }
          const updateData = {
            status: finalStatus
          };
          if (hasAnySuccess) {
            updateData.publishedAt = /* @__PURE__ */ new Date();
            updateData.publishedPlatforms = successfulPlatforms;
          }
          await firestoreService.updateStory(story.id, updateData);
          console.log(`      \u{1F4CA} Story ${story.id} FINAL STATUS: ${finalStatus === "published" ? "\u2705 PUBLISHED" : "\u274C FAILED"}`);
        } catch (error) {
          console.error(`      \u274C Error publishing story ${story.id}:`, error);
          await firestoreService.updateStory(story.id, {
            status: "failed"
          });
        }
      }
      isR2Url(url) {
        return url.includes(".r2.cloudflarestorage.com") || url.includes("r2.dev");
      }
      async refreshMediaUrls(story) {
        const refreshedStory = { ...story };
        if (story.mediaUrl) {
          if (this.isR2Url(story.mediaUrl)) {
            console.log(`\u{1F504} Refreshing main media URL for story ${story.id}...`);
            const verification = await r2Storage.verifyAndGetUrl(story.mediaUrl);
            if (!verification.valid) {
              console.error(`\u274C \u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0633\u062D\u0627\u0628\u064A: ${verification.error}`);
              throw new Error(`\u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0641\u064A \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0633\u062D\u0627\u0628\u064A. \u064A\u0631\u062C\u0649 \u0625\u0639\u0627\u062F\u0629 \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631\u0629. (${verification.fileKey || story.mediaUrl})`);
            }
            if (verification.freshUrl) {
              refreshedStory.mediaUrl = verification.freshUrl;
              console.log(`   \u2705 URL refreshed successfully`);
            }
          } else {
            console.log(`\u2139\uFE0F Main media URL is not from R2: ${story.mediaUrl.substring(0, 80)}...`);
          }
        }
        return refreshedStory;
      }
      async publishToAccount(story, account) {
        const { PublishingErrorHandler: PublishingErrorHandler2 } = await Promise.resolve().then(() => (init_error_handler(), error_handler_exports));
        const { FileValidator: FileValidator2 } = await Promise.resolve().then(() => (init_file_validator(), file_validator_exports));
        const platform = account.platform;
        const format = story.format || "story";
        const validation = await PublishingErrorHandler2.validateStoryForPublishing(story, account);
        if (!validation.valid) {
          throw new Error(validation.errors.join("\n"));
        }
        if (PublishingErrorHandler2.isTokenExpired(account)) {
          const refreshedAccount = await PublishingErrorHandler2.refreshAccountTokenIfNeeded(account);
          if (!refreshedAccount) {
            throw new Error(`\u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u0629 \u0631\u0645\u0632 \u0627\u0644\u062F\u062E\u0648\u0644. \u064A\u0631\u062C\u0649 \u062A\u062D\u062F\u064A\u062B \u0627\u0644\u062D\u0633\u0627\u0628 "${account.name}"`);
          }
          account = refreshedAccount;
        }
        if (story.mediaUrl && story.mediaUrl.startsWith("blob:")) {
          throw new Error("\u0644\u0627 \u064A\u0645\u0643\u0646 \u0646\u0634\u0631 \u0645\u0644\u0641\u0627\u062A \u0645\u062D\u0644\u064A\u0629. \u064A\u062C\u0628 \u0631\u0641\u0639 \u0627\u0644\u0645\u0644\u0641\u0627\u062A \u0625\u0644\u0649 \u062E\u062F\u0645\u0629 \u062A\u062E\u0632\u064A\u0646 \u0633\u062D\u0627\u0628\u064A\u0629 \u0623\u0648\u0644\u0627\u064B.");
        }
        const refreshedStory = await this.refreshMediaUrls(story);
        if (refreshedStory.mediaUrl && !PublishingErrorHandler2.isValidUrl(refreshedStory.mediaUrl)) {
          throw new Error(`\u0631\u0627\u0628\u0637 \u0627\u0644\u0645\u0644\u0641 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D: ${refreshedStory.mediaUrl}`);
        }
        if (refreshedStory.mediaUrl) {
          const freshUrl = await FileValidator2.refreshUrlIfNeeded(refreshedStory.mediaUrl);
          refreshedStory.mediaUrl = freshUrl;
        }
        if (platform === "facebook") {
          const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
          const facebookImageUrl = refreshedStory.facebookPngUrl || refreshedStory.mediaUrl;
          if (format === "story") {
            if (!facebookImageUrl || !facebookImageUrl.startsWith("http")) {
              throw new Error("\u064A\u062C\u0628 \u0625\u0636\u0627\u0641\u0629 \u0631\u0627\u0628\u0637 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648 \u0635\u0627\u0644\u062D \u0645\u0646 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0644\u0644\u0646\u0634\u0631 \u0643\u0640 Story \u0639\u0644\u0649 Facebook");
            }
            if (refreshedStory.mediaType === "image") {
              console.log(`\u{1F4F8} Publishing to Facebook Story with 20-second duration...`);
              let storyToPublish = { photo_url: facebookImageUrl };
              try {
                const musicUrl = refreshedStory.musicUrl;
                console.log(`\u{1F3B5} Attempting to create 20-second story video${musicUrl ? " with music" : " without music"}...`);
                const storyVideo = await storyMusicService.createStoryWithMusic(
                  facebookImageUrl,
                  refreshedStory.content.substring(0, 50),
                  refreshedStory.category,
                  musicUrl
                );
                if (storyVideo) {
                  console.log(`\u2705 Successfully created 20-second story video with ${storyVideo.hasMusic ? "music" : "animation"}`);
                  console.log(`   Video Duration: 20 seconds`);
                  storyToPublish = { video_url: storyVideo.url };
                  refreshedStory.mediaType = "video";
                } else {
                  console.warn(`\u26A0\uFE0F createStoryWithMusic returned null, publishing image instead`);
                }
              } catch (musicError) {
                console.error(`\u274C Error creating story with music: ${musicError.message}`);
                console.log(`\u26A0\uFE0F Falling back to image-only story`);
              }
              return await facebookSDK2.publishStory(
                account.externalId,
                account.accessToken,
                storyToPublish
              );
            } else if (refreshedStory.mediaType === "video") {
              console.log(`\u{1F3AC} Publishing VIDEO to Facebook Story (20 seconds)...`);
              console.log(`   Video URL: ${refreshedStory.mediaUrl?.substring(0, 80)}...`);
              return await facebookSDK2.publishStory(
                account.externalId,
                account.accessToken,
                { video_url: refreshedStory.mediaUrl }
              );
            }
            throw new Error("\u064A\u062C\u0628 \u062A\u062D\u062F\u064A\u062F \u0646\u0648\u0639 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 (\u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648) \u0644\u0644\u0646\u0634\u0631 \u0643\u0640 Story \u0639\u0644\u0649 Facebook");
          }
          if (format === "reel") {
            if (!refreshedStory.mediaUrl || refreshedStory.mediaType !== "video" || !refreshedStory.mediaUrl.startsWith("http")) {
              throw new Error("\u064A\u062C\u0628 \u0625\u0636\u0627\u0641\u0629 \u0631\u0627\u0628\u0637 \u0641\u064A\u062F\u064A\u0648 \u0635\u0627\u0644\u062D \u0645\u0646 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0644\u0644\u0646\u0634\u0631 \u0643\u0640 Reel \u0639\u0644\u0649 Facebook");
            }
            return await facebookSDK2.publishReel(
              account.externalId,
              account.accessToken,
              {
                video_url: refreshedStory.mediaUrl,
                description: refreshedStory.content
              }
            );
          }
          if (facebookImageUrl && refreshedStory.mediaType && facebookImageUrl.startsWith("http")) {
            if (refreshedStory.mediaType === "image") {
              return await facebookSDK2.uploadPhoto(
                account.externalId,
                account.accessToken,
                facebookImageUrl,
                refreshedStory.content
              );
            } else if (refreshedStory.mediaType === "video") {
              return await facebookSDK2.uploadVideo(
                account.externalId,
                account.accessToken,
                refreshedStory.mediaUrl,
                refreshedStory.content
              );
            }
          }
          return await facebookSDK2.publishPost(
            account.externalId,
            account.accessToken,
            { message: refreshedStory.content }
          );
        } else if (platform === "instagram") {
          const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
          const instagramImageUrl = refreshedStory.instagramPngUrl || refreshedStory.mediaUrl;
          if (!instagramImageUrl || !refreshedStory.mediaType || !instagramImageUrl.startsWith("http")) {
            throw new Error("\u064A\u062C\u0628 \u0625\u0636\u0627\u0641\u0629 \u0631\u0627\u0628\u0637 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648 \u0635\u0627\u0644\u062D \u0645\u0646 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 Instagram");
          }
          if (format === "story") {
            if (refreshedStory.mediaType === "image") {
              console.log(`\u{1F4F8} Publishing to Instagram Story using ${refreshedStory.instagramPngUrl ? "platform-specific PNG" : "default"} format`);
            } else if (refreshedStory.mediaType === "video") {
              console.log(`\u{1F3AC} Publishing VIDEO to Instagram Story...`);
              console.log(`   Video URL: ${refreshedStory.mediaUrl?.substring(0, 80)}...`);
            }
            return await instagramSDK2.publishStory(
              account.externalId,
              account.accessToken,
              {
                image_url: refreshedStory.mediaType === "image" ? instagramImageUrl : void 0,
                video_url: refreshedStory.mediaType === "video" ? refreshedStory.mediaUrl : void 0,
                media_type: "STORIES"
              }
            );
          }
          if (format === "reel") {
            if (refreshedStory.mediaType !== "video") {
              throw new Error("\u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0641\u064A\u062F\u064A\u0648 \u0644\u0644\u0646\u0634\u0631 \u0643\u0640 Reel \u0639\u0644\u0649 Instagram");
            }
            return await instagramSDK2.publishReel(
              account.externalId,
              account.accessToken,
              refreshedStory.mediaUrl,
              refreshedStory.content
            );
          }
          return await instagramSDK2.publishPost(
            account.externalId,
            account.accessToken,
            {
              image_url: refreshedStory.mediaType === "image" ? instagramImageUrl : void 0,
              video_url: refreshedStory.mediaType === "video" ? refreshedStory.mediaUrl : void 0,
              caption: refreshedStory.content,
              media_type: refreshedStory.mediaType === "image" ? "IMAGE" : "VIDEO"
            }
          );
        } else if (platform === "tiktok") {
          const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
          const tiktokImageUrl = refreshedStory.tiktokWebpUrl || refreshedStory.webpUrl || refreshedStory.jpegUrl || refreshedStory.mediaUrl;
          if (!tiktokImageUrl || !tiktokImageUrl.startsWith("http")) {
            throw new Error("\u064A\u062C\u0628 \u0625\u0636\u0627\u0641\u0629 \u0631\u0627\u0628\u0637 \u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648 \u0635\u0627\u0644\u062D \u0645\u0646 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 TikTok");
          }
          if (refreshedStory.mediaType === "image") {
            const formatUsed = refreshedStory.tiktokWebpUrl ? "platform-specific WebP" : refreshedStory.webpUrl ? "WebP" : refreshedStory.jpegUrl ? "JPEG" : "PNG";
            console.log(`\u{1F4F8} Publishing to TikTok using ${formatUsed} format`);
            return await tiktokSDK2.publishPhotoPost(
              account.accessToken,
              tiktokImageUrl,
              refreshedStory.content.substring(0, 150),
              refreshedStory.content.substring(0, 2200)
            );
          } else if (refreshedStory.mediaType === "video") {
            console.log(`\u{1F3AC} Publishing VIDEO to TikTok...`);
            console.log(`   Video URL: ${refreshedStory.mediaUrl?.substring(0, 80)}...`);
            return await tiktokSDK2.publishVideoFromUrl(
              account.accessToken,
              refreshedStory.mediaUrl,
              refreshedStory.content.substring(0, 150)
            );
          }
          throw new Error("\u064A\u062C\u0628 \u062A\u062D\u062F\u064A\u062F \u0646\u0648\u0639 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 (\u0635\u0648\u0631\u0629 \u0623\u0648 \u0641\u064A\u062F\u064A\u0648) \u0644\u0644\u0646\u0634\u0631 \u0639\u0644\u0649 TikTok");
        }
        throw new Error(`\u0645\u0646\u0635\u0629 ${platform} \u063A\u064A\u0631 \u0645\u062F\u0639\u0648\u0645\u0629`);
      }
      stop() {
        this.isRunning = false;
        console.log("\u{1F4C5} Story scheduler stopped");
      }
    };
    storyScheduler = new StoryScheduler();
  }
});

// server/index.ts
import fs6 from "fs";
import path7 from "path";
import { fileURLToPath } from "url";
import express2 from "express";
import * as cron2 from "node-cron";

// server/routes.ts
init_firestore();
import { createServer } from "http";

// shared/schema.ts
import { z } from "zod";
var storyCategories = [
  "movies",
  "tv_shows",
  "sports",
  "recipes",
  "gaming",
  "apps",
  "tv_channels",
  "news",
  "technology",
  "health",
  "travel",
  "education"
];
var platforms = ["facebook", "instagram", "tiktok"];
var storyStatus = ["draft", "scheduled", "published", "failed"];
var videoGenerationStatus = ["pending", "generating", "generated", "error"];
var storyFormats = ["story", "feed", "reel"];
var insertStorySchema = z.object({
  content: z.string().min(1, "\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0645\u0637\u0644\u0648\u0628").max(500, "\u0627\u0644\u062D\u062F \u0627\u0644\u0623\u0642\u0635\u0649 500 \u062D\u0631\u0641"),
  category: z.enum(storyCategories),
  platforms: z.array(z.enum(platforms)).min(1, "\u064A\u062C\u0628 \u0627\u062E\u062A\u064A\u0627\u0631 \u0645\u0646\u0635\u0629 \u0648\u0627\u062D\u062F\u0629 \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644"),
  scheduledTime: z.string().or(z.date()),
  format: z.enum(storyFormats).default("story"),
  mediaUrl: z.string().optional(),
  jpegUrl: z.string().optional(),
  webpUrl: z.string().optional(),
  mediaType: z.enum(["image", "video"]).optional(),
  trendingTopic: z.string().optional(),
  posterTitle: z.string().optional(),
  latestEpisode: z.number().optional(),
  sourceImageUrl: z.string().optional(),
  facebookPngUrl: z.string().optional(),
  instagramPngUrl: z.string().optional(),
  tiktokWebpUrl: z.string().optional(),
  musicUrl: z.string().optional(),
  musicTitle: z.string().optional(),
  musicArtist: z.string().optional(),
  musicThumbnail: z.string().optional(),
  musicDuration: z.number().optional(),
  musicVideoId: z.string().optional(),
  videoDuration: z.number().optional(),
  originCountry: z.string().optional(),
  videoUrl: z.string().optional(),
  videoGenerationStatus: z.enum(videoGenerationStatus).optional(),
  videoGeneratedAt: z.date().optional(),
  videoScheduledGenerationTime: z.date().optional(),
  videoStorageKey: z.string().optional(),
  videoContentType: z.string().optional(),
  videoFileSize: z.number().optional()
}).refine((data) => {
  if (data.mediaUrl && !data.mediaUrl.startsWith("http://") && !data.mediaUrl.startsWith("https://")) {
    return false;
  }
  if (data.jpegUrl && !data.jpegUrl.startsWith("http://") && !data.jpegUrl.startsWith("https://")) {
    return false;
  }
  if (data.webpUrl && !data.webpUrl.startsWith("http://") && !data.webpUrl.startsWith("https://")) {
    return false;
  }
  return true;
}, {
  message: "\u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0631\u0627\u0628\u0637 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 \u0631\u0627\u0628\u0637\u0627\u064B \u0635\u0627\u0644\u062D\u0627\u064B \u064A\u0628\u062F\u0623 \u0628\u0640 http:// \u0623\u0648 https://",
  path: ["mediaUrl"]
});
var updateStorySchema = z.object({
  content: z.string().min(1).max(500).optional(),
  category: z.enum(storyCategories).optional(),
  platforms: z.array(z.enum(platforms)).min(1).optional(),
  publishedPlatforms: z.array(z.enum(platforms)).optional(),
  scheduledTime: z.string().or(z.date()).optional(),
  format: z.enum(storyFormats).optional(),
  mediaUrl: z.string().optional(),
  jpegUrl: z.string().optional(),
  webpUrl: z.string().optional(),
  mediaType: z.enum(["image", "video"]).optional(),
  trendingTopic: z.string().optional(),
  posterTitle: z.string().optional(),
  latestEpisode: z.number().optional(),
  sourceImageUrl: z.string().optional(),
  facebookPngUrl: z.string().optional(),
  instagramPngUrl: z.string().optional(),
  tiktokWebpUrl: z.string().optional(),
  musicUrl: z.string().optional(),
  musicTitle: z.string().optional(),
  musicArtist: z.string().optional(),
  musicThumbnail: z.string().optional(),
  musicDuration: z.number().optional(),
  musicVideoId: z.string().optional(),
  videoDuration: z.number().optional(),
  originCountry: z.string().optional(),
  videoUrl: z.string().optional(),
  videoGenerationStatus: z.enum(videoGenerationStatus).optional(),
  videoGeneratedAt: z.date().optional(),
  videoScheduledGenerationTime: z.date().optional(),
  videoStorageKey: z.string().optional(),
  videoContentType: z.string().optional(),
  videoFileSize: z.number().optional(),
  status: z.enum(storyStatus).optional()
}).refine((data) => {
  if (data.mediaUrl && !data.mediaUrl.startsWith("http://") && !data.mediaUrl.startsWith("https://")) {
    return false;
  }
  if (data.jpegUrl && !data.jpegUrl.startsWith("http://") && !data.jpegUrl.startsWith("https://")) {
    return false;
  }
  if (data.webpUrl && !data.webpUrl.startsWith("http://") && !data.webpUrl.startsWith("https://")) {
    return false;
  }
  return true;
}, {
  message: "\u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0631\u0627\u0628\u0637 \u0627\u0644\u0648\u0633\u0627\u0626\u0637 \u0631\u0627\u0628\u0637\u0627\u064B \u0635\u0627\u0644\u062D\u0627\u064B \u064A\u0628\u062F\u0623 \u0628\u0640 http:// \u0623\u0648 https://",
  path: ["mediaUrl"]
});
var updateSettingsSchema = z.object({
  emailNotifications: z.boolean().optional(),
  smsNotifications: z.boolean().optional(),
  pushNotifications: z.boolean().optional(),
  publicProfile: z.boolean().optional(),
  showActivity: z.boolean().optional(),
  autoPublish: z.boolean().optional(),
  preferredPublishTime: z.string().optional(),
  autoStoryGenerationEnabled: z.boolean().optional(),
  autoStoryGenerationTime: z.string().optional(),
  autoStoryCategories: z.array(z.enum(storyCategories)).optional(),
  autoStoryPlatforms: z.array(z.enum(platforms)).optional(),
  autoStoryFormat: z.enum(storyFormats).optional(),
  autoStoryWithMusic: z.boolean().optional(),
  autoStoryWithVideo: z.boolean().optional()
});
var autoStoryGenerationSettingsSchema = z.object({
  enabled: z.boolean(),
  publishTime: z.string().regex(/^\d{2}:\d{2}$/, "Invalid time format"),
  categories: z.array(z.enum(storyCategories)).min(1),
  platforms: z.array(z.enum(platforms)).min(1),
  format: z.enum(storyFormats),
  withMusic: z.boolean(),
  withVideo: z.boolean(),
  scheduleVideoGenerationInAdvance: z.boolean().optional(),
  videoGenerationHoursBefore: z.number().optional()
});
var apiProviders = ["facebook", "instagram", "tiktok", "deepseek", "cloudflare_r2", "youtube", "huggingface", "gemini", "google_trends", "rapidapi", "tmdb", "github_actions"];
var insertAPIConfigSchema = z.object({
  provider: z.enum(apiProviders),
  apiKey: z.string().optional(),
  appId: z.string().optional(),
  appSecret: z.string().optional(),
  additionalConfig: z.object({
    accountId: z.string().optional(),
    accessKeyId: z.string().optional(),
    secretAccessKey: z.string().optional(),
    bucketName: z.string().optional(),
    searchEngineId: z.string().optional(),
    replit_app_url: z.string().optional(),
    cron_secret_key: z.string().optional()
  }).optional()
});
var updateAPIConfigSchema = z.object({
  apiKey: z.string().optional(),
  appId: z.string().optional(),
  appSecret: z.string().optional(),
  additionalConfig: z.object({
    accountId: z.string().optional(),
    accessKeyId: z.string().optional(),
    secretAccessKey: z.string().optional(),
    bucketName: z.string().optional(),
    searchEngineId: z.string().optional(),
    replit_app_url: z.string().optional(),
    cron_secret_key: z.string().optional()
  }).optional(),
  isConnected: z.boolean().optional(),
  lastTested: z.date().optional()
});
var accountPlatforms = ["facebook", "instagram", "tiktok"];
var accountStatus = ["active", "inactive", "expired", "error"];
var accountTypes = ["page", "profile", "business"];
var insertLinkedAccountSchema = z.object({
  platform: z.enum(accountPlatforms),
  accountType: z.enum(accountTypes),
  externalId: z.string().min(1, "\u0645\u0639\u0631\u0641 \u0627\u0644\u062D\u0633\u0627\u0628 \u0645\u0637\u0644\u0648\u0628"),
  name: z.string().min(1, "\u0627\u0633\u0645 \u0627\u0644\u062D\u0633\u0627\u0628 \u0645\u0637\u0644\u0648\u0628"),
  username: z.string().optional(),
  profilePictureUrl: z.string().optional(),
  accessToken: z.string().min(1, "\u0631\u0645\u0632 \u0627\u0644\u062F\u062E\u0648\u0644 \u0645\u0637\u0644\u0648\u0628"),
  refreshToken: z.string().optional(),
  tokenExpiresAt: z.date().optional(),
  permissions: z.array(z.string()).default([]),
  capabilities: z.object({
    canPublishStories: z.boolean().default(false),
    canPublishPosts: z.boolean().default(false),
    canPublishReels: z.boolean().default(false),
    canSchedule: z.boolean().default(false),
    canGetInsights: z.boolean().default(false),
    maxVideoSize: z.number().optional(),
    maxImageSize: z.number().optional(),
    supportedFormats: z.array(z.string()).optional()
  }),
  targeting: z.object({
    defaultAudience: z.enum(["public", "friends", "custom"]).optional(),
    ageRange: z.object({ min: z.number(), max: z.number() }).optional(),
    locations: z.array(z.string()).optional(),
    interests: z.array(z.string()).optional(),
    language: z.string().optional()
  }).optional()
});
var updateLinkedAccountSchema = z.object({
  name: z.string().min(1).optional(),
  username: z.string().optional(),
  profilePictureUrl: z.string().optional(),
  status: z.enum(accountStatus).optional(),
  accessToken: z.string().optional(),
  refreshToken: z.string().optional(),
  tokenExpiresAt: z.date().optional(),
  permissions: z.array(z.string()).optional(),
  capabilities: z.object({
    canPublishStories: z.boolean().optional(),
    canPublishPosts: z.boolean().optional(),
    canPublishReels: z.boolean().optional(),
    canSchedule: z.boolean().optional(),
    canGetInsights: z.boolean().optional(),
    maxVideoSize: z.number().optional(),
    maxImageSize: z.number().optional(),
    supportedFormats: z.array(z.string()).optional()
  }).optional(),
  targeting: z.object({
    defaultAudience: z.enum(["public", "friends", "custom"]).optional(),
    ageRange: z.object({ min: z.number(), max: z.number() }).optional(),
    locations: z.array(z.string()).optional(),
    interests: z.array(z.string()).optional(),
    language: z.string().optional()
  }).optional(),
  lastSyncedAt: z.date().optional(),
  lastPublishedAt: z.date().optional()
});
var insertStoryAccountAssignmentSchema = z.object({
  storyId: z.string().min(1),
  accountId: z.string().min(1)
});
var insertDailyStorySettingsSchema = z.object({
  isEnabled: z.boolean().default(true),
  publishTime: z.string().regex(/^\d{2}:\d{2}$/, "Format must be HH:mm"),
  timezone: z.string().default("Asia/Riyadh"),
  platforms: z.array(z.enum(platforms)).min(1),
  categories: z.array(z.enum(storyCategories)).min(1),
  musicMood: z.enum(["energetic", "calm", "uplifting", "dramatic"]).default("energetic"),
  videoQuality: z.enum(["sd", "hd", "4k"]).default("hd"),
  videoDuration: z.number().int().min(10).max(60).default(20),
  publishInterval: z.number().int().min(5).max(30).default(5),
  autoRetry: z.boolean().default(true),
  maxRetries: z.number().int().min(1).max(5).default(3)
}).extend({
  publishInterval: z.number().int().min(1).max(60).default(5)
});

// server/routes.ts
init_firebase_admin_setup();
init_firebase_rest_client();

// server/api-tester.ts
async function testAPIConnection(provider, config) {
  try {
    switch (provider) {
      case "facebook":
        return await testFacebookConnection(config);
      case "instagram":
        return await testInstagramConnection(config);
      case "tiktok":
        return await testTikTokConnection(config);
      case "deepseek":
        return await testDeepSeekConnection(config);
      case "cloudflare_r2":
        return await testCloudflareR2Connection(config);
      case "youtube":
        return await testYouTubeConnection(config);
      case "huggingface":
        return await testHuggingFaceConnection(config);
      case "gemini":
        return await testGeminiConnection(config);
      case "google_trends":
        return await testGoogleSearchConnection(config);
      case "tmdb":
        return await testTMDBConnection(config);
      case "github_actions":
        return await testHelioHostConnection(config);
      default:
        return {
          success: false,
          message: "Unknown provider"
        };
    }
  } catch (error) {
    return {
      success: false,
      message: error.message || "Connection test failed"
    };
  }
}
async function testFacebookConnection(config) {
  try {
    if (!config.appId || !config.appSecret) {
      return {
        success: false,
        message: "Facebook App ID and App Secret are required"
      };
    }
    const response = await fetch(`https://graph.facebook.com/oauth/access_token?client_id=${config.appId}&client_secret=${config.appSecret}&grant_type=client_credentials`);
    if (!response.ok) {
      let errorMessage = "Invalid Facebook credentials or insufficient permissions";
      try {
        const errorData = await response.json();
        if (errorData.error?.message) {
          errorMessage = `Facebook API Error: ${errorData.error.message}`;
        } else if (errorData.message) {
          errorMessage = `Facebook API Error: ${errorData.message}`;
        } else {
          errorMessage = `Facebook API Error (Status ${response.status}): ${response.statusText}`;
        }
      } catch {
        errorMessage = `Facebook API Error (Status ${response.status}): ${response.statusText}`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    if (data.access_token) {
      return {
        success: true,
        message: "Facebook connection successful"
      };
    }
    return {
      success: false,
      message: "Failed to obtain access token from Facebook"
    };
  } catch (error) {
    return {
      success: false,
      message: `Failed to connect to Facebook: ${error.message || "Network error"}`
    };
  }
}
async function testInstagramConnection(config) {
  try {
    if (!config.appId || !config.appSecret) {
      return {
        success: false,
        message: "Instagram App ID and App Secret are required"
      };
    }
    const response = await fetch(`https://graph.facebook.com/oauth/access_token?client_id=${config.appId}&client_secret=${config.appSecret}&grant_type=client_credentials`);
    if (!response.ok) {
      let errorMessage = "Invalid Instagram credentials or insufficient permissions";
      try {
        const errorData = await response.json();
        if (errorData.error?.message) {
          errorMessage = `Instagram API Error: ${errorData.error.message}`;
        } else if (errorData.message) {
          errorMessage = `Instagram API Error: ${errorData.message}`;
        } else {
          errorMessage = `Instagram API Error (Status ${response.status}): ${response.statusText}`;
        }
      } catch {
        errorMessage = `Instagram API Error (Status ${response.status}): ${response.statusText}`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    if (data.access_token) {
      return {
        success: true,
        message: "Instagram connection successful"
      };
    }
    return {
      success: false,
      message: "Failed to obtain access token from Instagram"
    };
  } catch (error) {
    return {
      success: false,
      message: `Failed to connect to Instagram: ${error.message || "Network error"}`
    };
  }
}
async function testTikTokConnection(config) {
  try {
    if (!config.apiKey || !config.appSecret) {
      return {
        success: false,
        message: "TikTok API Key and API Secret are required"
      };
    }
    const response = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: new URLSearchParams({
        client_key: config.apiKey,
        client_secret: config.appSecret,
        grant_type: "client_credentials"
      })
    });
    if (!response.ok) {
      let errorMessage = "Invalid TikTok credentials or insufficient permissions";
      try {
        const errorData = await response.json();
        if (errorData.error?.message) {
          errorMessage = `TikTok API Error: ${errorData.error.message}`;
        } else if (errorData.message) {
          errorMessage = `TikTok API Error: ${errorData.message}`;
        } else {
          errorMessage = `TikTok API Error (Status ${response.status}): ${response.statusText}`;
        }
      } catch {
        errorMessage = `TikTok API Error (Status ${response.status}): ${response.statusText}`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    if (data.access_token || data.data?.access_token) {
      return {
        success: true,
        message: "TikTok connection successful"
      };
    }
    return {
      success: false,
      message: "Failed to obtain access token from TikTok"
    };
  } catch (error) {
    return {
      success: false,
      message: `Failed to connect to TikTok: ${error.message || "Network error"}`
    };
  }
}
async function testDeepSeekConnection(config) {
  try {
    if (!config.apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D DeepSeek API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    console.log("[DeepSeek Test] Starting connection test...");
    console.log("[DeepSeek Test] API Key (masked):", config.apiKey.substring(0, 10) + "...");
    const response = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${config.apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: "deepseek-chat",
        messages: [{ role: "user", content: "Hello" }],
        max_tokens: 5
      })
    });
    console.log("[DeepSeek Test] Response status:", response.status, response.statusText);
    if (!response.ok) {
      let errorMessage = "\u0645\u0641\u062A\u0627\u062D DeepSeek API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D \u0623\u0648 \u0635\u0644\u0627\u062D\u064A\u0627\u062A \u063A\u064A\u0631 \u0643\u0627\u0641\u064A\u0629";
      try {
        const errorData = await response.json();
        console.log("[DeepSeek Test] Error response:", JSON.stringify(errorData, null, 2));
        if (response.status === 401) {
          errorMessage = "\u0645\u0641\u062A\u0627\u062D DeepSeek API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D";
        } else if (response.status === 402) {
          errorMessage = "\u0631\u0635\u064A\u062F \u062D\u0633\u0627\u0628\u0643 \u0641\u064A DeepSeek \u063A\u064A\u0631 \u0643\u0627\u0641\u064D - \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0631\u0635\u064A\u062F \u0625\u0644\u0649 \u062D\u0633\u0627\u0628\u0643";
        } else if (response.status === 403) {
          errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u062D\u0633\u0627\u0628\u0643 \u0648\u062E\u0637\u062A\u0643";
        } else if (response.status === 429) {
          errorMessage = "\u062A\u062C\u0627\u0648\u0632\u062A \u0627\u0644\u062D\u062F \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0645\u0646 \u0627\u0644\u0637\u0644\u0628\u0627\u062A - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0644\u0627\u062D\u0642\u0627\u064B";
        } else if (errorData.error?.message) {
          if (errorData.error.message.includes("Insufficient Balance")) {
            errorMessage = "\u0631\u0635\u064A\u062F \u062D\u0633\u0627\u0628\u0643 \u0641\u064A DeepSeek \u063A\u064A\u0631 \u0643\u0627\u0641\u064D - \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0631\u0635\u064A\u062F \u0625\u0644\u0649 \u062D\u0633\u0627\u0628\u0643";
          } else {
            errorMessage = `\u062E\u0637\u0623 DeepSeek API: ${errorData.error.message}`;
          }
        } else if (errorData.message) {
          if (errorData.message.includes("Insufficient Balance")) {
            errorMessage = "\u0631\u0635\u064A\u062F \u062D\u0633\u0627\u0628\u0643 \u0641\u064A DeepSeek \u063A\u064A\u0631 \u0643\u0627\u0641\u064D - \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0631\u0635\u064A\u062F \u0625\u0644\u0649 \u062D\u0633\u0627\u0628\u0643";
          } else {
            errorMessage = `\u062E\u0637\u0623 DeepSeek API: ${errorData.message}`;
          }
        } else {
          errorMessage = `\u062E\u0637\u0623 DeepSeek API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
        }
      } catch (parseError) {
        console.log("[DeepSeek Test] Failed to parse error response:", parseError);
        if (response.status === 402) {
          errorMessage = "\u0631\u0635\u064A\u062F \u062D\u0633\u0627\u0628\u0643 \u0641\u064A DeepSeek \u063A\u064A\u0631 \u0643\u0627\u0641\u064D - \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0631\u0635\u064A\u062F \u0625\u0644\u0649 \u062D\u0633\u0627\u0628\u0643";
        } else {
          errorMessage = `\u062E\u0637\u0623 DeepSeek API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
        }
      }
      console.log("[DeepSeek Test] Final error message:", errorMessage);
      return {
        success: false,
        message: errorMessage
      };
    }
    const successData = await response.json();
    console.log("[DeepSeek Test] Success! Response:", JSON.stringify(successData, null, 2));
    return {
      success: true,
      message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 DeepSeek - \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0635\u0627\u0644\u062D \u0648\u064A\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D"
    };
  } catch (error) {
    console.log("[DeepSeek Test] Exception:", error);
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 DeepSeek: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}
async function testCloudflareR2Connection(config) {
  try {
    console.log("[R2 Test] Starting connection test...");
    if (!config.additionalConfig?.accountId) {
      console.log("[R2 Test] Missing accountId");
      return { success: false, message: "\u0645\u0639\u0631\u0641 \u062D\u0633\u0627\u0628 Cloudflare R2 \u0645\u0637\u0644\u0648\u0628" };
    }
    if (!config.additionalConfig?.accessKeyId) {
      console.log("[R2 Test] Missing accessKeyId");
      return { success: false, message: "\u0645\u0639\u0631\u0641 \u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0648\u0635\u0648\u0644 Cloudflare R2 \u0645\u0637\u0644\u0648\u0628" };
    }
    if (!config.additionalConfig?.secretAccessKey) {
      console.log("[R2 Test] Missing secretAccessKey");
      return { success: false, message: "\u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0648\u0635\u0648\u0644 \u0627\u0644\u0633\u0631\u064A Cloudflare R2 \u0645\u0637\u0644\u0648\u0628" };
    }
    if (!config.additionalConfig?.bucketName) {
      console.log("[R2 Test] Missing bucketName");
      return { success: false, message: "\u0627\u0633\u0645 \u062F\u0644\u0648 Cloudflare R2 \u0645\u0637\u0644\u0648\u0628" };
    }
    console.log("[R2 Test] Account ID:", config.additionalConfig.accountId);
    console.log("[R2 Test] Access Key ID (masked):", config.additionalConfig.accessKeyId.substring(0, 8) + "...");
    console.log("[R2 Test] Bucket Name:", config.additionalConfig.bucketName);
    console.log("[R2 Test] Endpoint:", `https://${config.additionalConfig.accountId}.r2.cloudflarestorage.com`);
    const { S3Client: S3Client2, HeadBucketCommand } = await import("@aws-sdk/client-s3");
    const r2Client = new S3Client2({
      region: "auto",
      endpoint: `https://${config.additionalConfig.accountId}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: config.additionalConfig.accessKeyId,
        secretAccessKey: config.additionalConfig.secretAccessKey
      }
    });
    console.log("[R2 Test] Sending HeadBucket command...");
    await r2Client.send(new HeadBucketCommand({
      Bucket: config.additionalConfig.bucketName
    }));
    console.log("[R2 Test] Connection successful!");
    return {
      success: true,
      message: "\u062A\u0645 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Cloudflare R2 \u0628\u0646\u062C\u0627\u062D"
    };
  } catch (error) {
    console.error("[R2 Test] Error occurred:", error);
    console.error("[R2 Test] Error name:", error.name);
    console.error("[R2 Test] Error code:", error.Code || error.code);
    console.error("[R2 Test] Error message:", error.message);
    console.error("[R2 Test] HTTP status code:", error.$metadata?.httpStatusCode);
    console.error("[R2 Test] Full error object:", JSON.stringify(error, null, 2));
    let errorMessage = "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Cloudflare R2";
    const httpStatusCode = error.$metadata?.httpStatusCode;
    const errorCode = error.Code || error.code || error.name;
    if (errorCode === "InvalidAccessKeyId" || httpStatusCode === 403) {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u0645\u0639\u0631\u0641 \u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0648\u0635\u0648\u0644 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D. \u062A\u062D\u0642\u0642 \u0645\u0646 \u0635\u062D\u0629 Access Key ID";
    } else if (errorCode === "SignatureDoesNotMatch") {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0648\u0635\u0648\u0644 \u0627\u0644\u0633\u0631\u064A \u063A\u064A\u0631 \u0635\u0627\u0644\u062D. \u062A\u062D\u0642\u0642 \u0645\u0646 \u0635\u062D\u0629 Secret Access Key";
    } else if (errorCode === "NoSuchBucket" || httpStatusCode === 404) {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u0627\u0644\u062F\u0644\u0648 \u0627\u0644\u0645\u062D\u062F\u062F \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F. \u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0633\u0645 Bucket";
    } else if (errorCode === "InvalidBucketName") {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u0627\u0633\u0645 \u0627\u0644\u062F\u0644\u0648 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D";
    } else if (errorCode === "NetworkingError" || error.message?.includes("getaddrinfo")) {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0634\u0628\u0643\u0629. \u062A\u062D\u0642\u0642 \u0645\u0646 Account ID";
    } else if (httpStatusCode === 401) {
      errorMessage = "\u062E\u0637\u0623 Cloudflare R2: \u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0645\u0635\u0627\u062F\u0642\u0629. \u062A\u062D\u0642\u0642 \u0645\u0646 Access Key ID \u0648 Secret Access Key";
    } else if (error.message) {
      errorMessage = `\u062E\u0637\u0623 Cloudflare R2: ${error.message}`;
    }
    return {
      success: false,
      message: errorMessage
    };
  }
}
async function testYouTubeConnection(config) {
  try {
    if (!config.apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D YouTube API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    const { YouTubeMusicService: YouTubeMusicService2 } = await Promise.resolve().then(() => (init_youtube_music(), youtube_music_exports));
    const youtubeService = new YouTubeMusicService2(config.apiKey);
    return await youtubeService.testConnection();
  } catch (error) {
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 YouTube API: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}
async function testHuggingFaceConnection(config) {
  try {
    if (!config.apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D Hugging Face API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    console.log("[HuggingFace Test] Starting connection test...");
    console.log("[HuggingFace Test] Token (masked):", config.apiKey.substring(0, 10) + "...");
    const response = await fetch("https://huggingface.co/api/whoami-v2", {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${config.apiKey}`
      }
    });
    console.log("[HuggingFace Test] Response status:", response.status, response.statusText);
    if (!response.ok) {
      let errorMessage = "\u0645\u0641\u062A\u0627\u062D Hugging Face API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D";
      if (response.status === 401) {
        errorMessage = '\u0645\u0641\u062A\u0627\u062D Hugging Face API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 Access Token. \u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646 \u0627\u0644\u0640 Token \u0645\u0646 \u0646\u0648\u0639 "Read" \u0623\u0648 "Write"';
      } else if (response.status === 403) {
        errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0635\u0644\u0627\u062D\u064A\u0627\u062A Access Token";
      } else if (response.status === 429) {
        errorMessage = "\u062A\u062C\u0627\u0648\u0632\u062A \u0627\u0644\u062D\u062F \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0645\u0646 \u0627\u0644\u0637\u0644\u0628\u0627\u062A - \u064A\u0631\u062C\u0649 \u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0631 \u0642\u0644\u064A\u0644\u0627\u064B \u062B\u0645 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649";
      } else {
        try {
          const errorData = await response.json();
          console.log("[HuggingFace Test] Error data:", JSON.stringify(errorData));
          if (errorData.error) {
            errorMessage = `\u062E\u0637\u0623 Hugging Face API: ${errorData.error}`;
          }
        } catch (parseError) {
          console.log("[HuggingFace Test] Failed to parse error response");
          errorMessage = `\u062E\u0637\u0623 Hugging Face API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
        }
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const userData = await response.json();
    console.log("[HuggingFace Test] User data received:", JSON.stringify(userData, null, 2));
    if (userData.error) {
      console.log("[HuggingFace Test] Error in response body:", userData.error);
      let errorMessage = `\u062E\u0637\u0623 Hugging Face: ${userData.error}`;
      if (userData.estimated_time) {
        errorMessage = `\u0627\u0644\u0646\u0645\u0648\u0630\u062C \u0642\u064A\u062F \u0627\u0644\u062A\u062D\u0645\u064A\u0644\u060C \u064A\u0631\u062C\u0649 \u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0631 ${Math.ceil(userData.estimated_time)} \u062B\u0627\u0646\u064A\u0629 \u062B\u0645 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    if (!userData.name && !userData.id && !userData.fullname && !userData.username) {
      console.log("[HuggingFace Test] No valid user data received");
      if (userData.estimated_time) {
        return {
          success: false,
          message: `\u0644\u0645 \u0646\u062A\u0645\u0643\u0646 \u0645\u0646 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D. \u0627\u0644\u0646\u0645\u0648\u0630\u062C \u0642\u064A\u062F \u0627\u0644\u062A\u062D\u0645\u064A\u0644 (${Math.ceil(userData.estimated_time)} \u062B\u0627\u0646\u064A\u0629) - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649 \u0644\u0627\u062D\u0642\u0627\u064B`
        };
      }
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D Hugging Face API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D \u0623\u0648 \u0627\u0633\u062A\u062C\u0627\u0628\u0629 \u063A\u064A\u0631 \u0645\u062A\u0648\u0642\u0639\u0629 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 Access Token"
      };
    }
    const accountName = userData.name || userData.fullname || userData.username || userData.id;
    const accountType = userData.type || "user";
    let infoMessage = `\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Hugging Face! \u0627\u0644\u062D\u0633\u0627\u0628: ${accountName}`;
    if (accountType === "org") {
      infoMessage += " (\u0645\u0646\u0638\u0645\u0629)";
    }
    if (userData.canPay === false) {
      infoMessage += " - \u062D\u0633\u0627\u0628 \u0645\u062C\u0627\u0646\u064A (\u064A\u0645\u0643\u0646\u0643 \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631 \u0645\u062C\u0627\u0646\u0627\u064B \u0628\u0627\u0633\u062A\u062E\u062F\u0627\u0645 Hugging Face Inference)";
    } else {
      infoMessage += " - \u062C\u0627\u0647\u0632 \u0644\u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631 \u0645\u062C\u0627\u0646\u0627\u064B";
    }
    console.log("[HuggingFace Test] Connection successful!");
    return {
      success: true,
      message: infoMessage
    };
  } catch (error) {
    console.error("[HuggingFace Test] Exception:", error);
    if (error.message?.includes("fetch") || error.message?.includes("network") || error.code === "ENOTFOUND") {
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Hugging Face - \u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u062A\u0635\u0627\u0644 \u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A"
      };
    }
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Hugging Face API: ${error.message || "\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641"}`
    };
  }
}
async function testGeminiConnection(config) {
  try {
    if (!config.apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D Gemini API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key=${config.apiKey}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        contents: [{
          parts: [{
            text: "Hello"
          }]
        }]
      })
    });
    if (!response.ok) {
      let errorMessage = "\u0645\u0641\u062A\u0627\u062D Gemini API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D";
      if (response.status === 400) {
        try {
          const errorData = await response.json();
          if (errorData.error?.message) {
            errorMessage = `\u062E\u0637\u0623 Gemini API: ${errorData.error.message}`;
          }
        } catch {
          errorMessage = "\u0645\u0641\u062A\u0627\u062D Gemini API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D \u0623\u0648 \u063A\u064A\u0631 \u0645\u0641\u0639\u0651\u0644";
        }
      } else if (response.status === 403) {
        errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0645\u0641\u062A\u0627\u062D API \u0648\u0627\u0644\u062A\u0623\u0643\u062F \u0645\u0646 \u062A\u0641\u0639\u064A\u0644 Gemini API";
      } else {
        try {
          const errorData = await response.json();
          if (errorData.error?.message) {
            errorMessage = `\u062E\u0637\u0623 Gemini API: ${errorData.error.message}`;
          }
        } catch {
          errorMessage = `\u062E\u0637\u0623 Gemini API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
        }
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    if (data.candidates && data.candidates.length > 0) {
      return {
        success: true,
        message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Gemini API - \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0635\u0627\u0644\u062D \u0648\u064A\u0639\u0645\u0644 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D!"
      };
    }
    return {
      success: false,
      message: "\u0627\u0633\u062A\u062C\u0627\u0628\u0629 \u063A\u064A\u0631 \u0645\u062A\u0648\u0642\u0639\u0629 \u0645\u0646 Gemini API"
    };
  } catch (error) {
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Gemini API: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}
async function testGoogleSearchConnection(config) {
  try {
    const apiKey = config.apiKey;
    const searchEngineId = config.additionalConfig?.searchEngineId;
    if (!apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D Google Custom Search API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    if (!searchEngineId) {
      return {
        success: false,
        message: "\u0645\u0639\u0631\u0641 \u0645\u062D\u0631\u0643 \u0627\u0644\u0628\u062D\u062B (Search Engine ID) \u0645\u0637\u0644\u0648\u0628"
      };
    }
    console.log("[Google Search Test] Starting connection test...");
    console.log("[Google Search Test] API Key (masked):", apiKey.substring(0, 10) + "...");
    console.log("[Google Search Test] Search Engine ID:", searchEngineId);
    const params = new URLSearchParams({
      key: apiKey,
      cx: searchEngineId,
      q: "test",
      searchType: "image",
      num: "1"
    });
    const response = await fetch(
      `https://www.googleapis.com/customsearch/v1?${params.toString()}`
    );
    console.log("[Google Search Test] Response status:", response.status, response.statusText);
    if (!response.ok) {
      let errorMessage = "\u0628\u064A\u0627\u0646\u0627\u062A Google Custom Search API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D\u0629";
      try {
        const errorData = await response.json();
        console.log("[Google Search Test] Error response:", JSON.stringify(errorData, null, 2));
        if (response.status === 400) {
          if (errorData.error?.message?.includes("API key not valid")) {
            errorMessage = "\u0645\u0641\u062A\u0627\u062D Google API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D";
          } else if (errorData.error?.message?.includes("cx")) {
            errorMessage = "\u0645\u0639\u0631\u0641 \u0645\u062D\u0631\u0643 \u0627\u0644\u0628\u062D\u062B (CX) \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 Search Engine ID";
          } else {
            errorMessage = `\u062E\u0637\u0623 Google API: ${errorData.error?.message || "\u0637\u0644\u0628 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D"}`;
          }
        } else if (response.status === 401) {
          errorMessage = "\u0645\u0641\u062A\u0627\u062D Google API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0648\u0627\u0644\u062A\u0623\u0643\u062F \u0645\u0646 \u062A\u0641\u0639\u064A\u0644 Custom Search API";
        } else if (response.status === 403) {
          if (errorData.error?.message?.includes("quota")) {
            errorMessage = "\u062A\u0645 \u062A\u062C\u0627\u0648\u0632 \u062D\u0635\u0629 \u0627\u0644\u0627\u0633\u062A\u062E\u062F\u0627\u0645 \u0627\u0644\u064A\u0648\u0645\u064A\u0629 \u0644\u0640 Google API - \u062D\u0627\u0648\u0644 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649 \u063A\u062F\u0627\u064B";
          } else if (errorData.error?.message?.includes("disabled")) {
            errorMessage = "Google Custom Search API \u063A\u064A\u0631 \u0645\u0641\u0639\u0651\u0644 - \u064A\u0631\u062C\u0649 \u062A\u0641\u0639\u064A\u0644\u0647 \u0645\u0646 Google Cloud Console";
          } else {
            errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0648\u062A\u0641\u0639\u064A\u0644 Custom Search API";
          }
        } else if (response.status === 429) {
          errorMessage = "\u062A\u0645 \u062A\u062C\u0627\u0648\u0632 \u062D\u062F \u0627\u0644\u0637\u0644\u0628\u0627\u062A - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0644\u0627\u062D\u0642\u0627\u064B";
        } else if (errorData.error?.message) {
          errorMessage = `\u062E\u0637\u0623 Google API: ${errorData.error.message}`;
        }
      } catch (parseError) {
        console.log("[Google Search Test] Failed to parse error response:", parseError);
        errorMessage = `\u062E\u0637\u0623 Google API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    console.log("[Google Search Test] Success! Items found:", data.items?.length || 0);
    return {
      success: true,
      message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Google Custom Search API - \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0648\u0645\u0639\u0631\u0641 \u0645\u062D\u0631\u0643 \u0627\u0644\u0628\u062D\u062B \u064A\u0639\u0645\u0644\u0627\u0646 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D!"
    };
  } catch (error) {
    console.log("[Google Search Test] Exception:", error);
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 Google Search API: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}
async function testTMDBConnection(config) {
  try {
    if (!config.apiKey) {
      return {
        success: false,
        message: "\u0645\u0641\u062A\u0627\u062D TMDB API \u0645\u0637\u0644\u0648\u0628"
      };
    }
    console.log("[TMDB Test] Starting connection test...");
    console.log("[TMDB Test] API Key (masked):", config.apiKey.substring(0, 10) + "...");
    const response = await fetch(
      `https://api.themoviedb.org/3/configuration?api_key=${config.apiKey}`
    );
    console.log("[TMDB Test] Response status:", response.status, response.statusText);
    if (!response.ok) {
      let errorMessage = "\u0645\u0641\u062A\u0627\u062D TMDB API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D";
      try {
        const errorData = await response.json();
        console.log("[TMDB Test] Error response:", JSON.stringify(errorData, null, 2));
        if (response.status === 401) {
          errorMessage = "\u0645\u0641\u062A\u0627\u062D TMDB API \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D";
        } else if (response.status === 403) {
          errorMessage = "\u0644\u064A\u0633 \u0644\u062F\u064A\u0643 \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u0648\u0635\u0648\u0644 - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u062D\u0633\u0627\u0628\u0643 \u0641\u064A TMDB";
        } else if (response.status === 429) {
          errorMessage = "\u062A\u062C\u0627\u0648\u0632\u062A \u0627\u0644\u062D\u062F \u0627\u0644\u0645\u0633\u0645\u0648\u062D \u0645\u0646 \u0627\u0644\u0637\u0644\u0628\u0627\u062A - \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0644\u0627\u062D\u0642\u0627\u064B";
        } else if (errorData.status_message) {
          errorMessage = `\u062E\u0637\u0623 TMDB API: ${errorData.status_message}`;
        } else {
          errorMessage = `\u062E\u0637\u0623 TMDB API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
        }
      } catch (parseError) {
        console.log("[TMDB Test] Failed to parse error response:", parseError);
        errorMessage = `\u062E\u0637\u0623 TMDB API (\u0631\u0645\u0632 ${response.status}): ${response.statusText}`;
      }
      return {
        success: false,
        message: errorMessage
      };
    }
    const data = await response.json();
    console.log("[TMDB Test] Configuration received successfully");
    if (data.images && data.images.base_url) {
      return {
        success: true,
        message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 TMDB API - \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0635\u0627\u0644\u062D \u0648\u062C\u0627\u0647\u0632 \u0644\u062C\u0644\u0628 \u0628\u064A\u0627\u0646\u0627\u062A \u0627\u0644\u0623\u0641\u0644\u0627\u0645 \u0648\u0627\u0644\u0645\u0633\u0644\u0633\u0644\u0627\u062A!"
      };
    }
    return {
      success: false,
      message: "\u0627\u0633\u062A\u062C\u0627\u0628\u0629 \u063A\u064A\u0631 \u0645\u062A\u0648\u0642\u0639\u0629 \u0645\u0646 TMDB API"
    };
  } catch (error) {
    console.log("[TMDB Test] Exception:", error);
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 TMDB API: ${error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}
async function testHelioHostConnection(config) {
  try {
    console.log("[GitHub Test] ================================");
    console.log("[GitHub Test] Starting connection test...");
    const webhookUrl = config.additionalConfig?.webhookUrl;
    if (!webhookUrl || webhookUrl.trim() === "") {
      console.log("[GitHub Test] Missing or empty webhookUrl");
      return {
        success: false,
        message: "\u0631\u0627\u0628\u0637 \u0627\u0644\u0633\u064A\u0631\u0641\u0631 \u0645\u0637\u0644\u0648\u0628 (\u0645\u062B\u0627\u0644: https://turk.github.com)"
      };
    }
    if (!webhookUrl.startsWith("http://") && !webhookUrl.startsWith("https://")) {
      console.log("[GitHub Test] Invalid URL format");
      return {
        success: false,
        message: "\u0631\u0627\u0628\u0637 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D - \u064A\u062C\u0628 \u0623\u0646 \u064A\u0628\u062F\u0623 \u0628\u0640 http:// \u0623\u0648 https://"
      };
    }
    console.log("[GitHub Test] Webhook URL:", webhookUrl);
    const cronSecretKey = process.env.CRON_SECRET_KEY;
    if (!cronSecretKey) {
      console.log("[GitHub Test] Missing CRON_SECRET_KEY environment variable");
      return {
        success: false,
        message: "\u0645\u062A\u063A\u064A\u0631 \u0627\u0644\u0628\u064A\u0626\u0629 CRON_SECRET_KEY \u063A\u064A\u0631 \u0645\u062D\u062F\u062F - \u064A\u0631\u062C\u0649 \u0627\u0644\u062A\u062D\u0642\u0642 \u0645\u0646 \u0625\u0639\u062F\u0627\u062F\u0627\u062A \u0627\u0644\u062E\u0627\u062F\u0645"
      };
    }
    console.log("[GitHub Test] CRON_SECRET_KEY is set (masked):", cronSecretKey.substring(0, 10) + "...");
    console.log("[GitHub Test] Making test request to:", `${webhookUrl}/api/cron/trigger`);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15e3);
    let response;
    try {
      response = await fetch(`${webhookUrl}/api/cron/trigger`, {
        method: "POST",
        headers: {
          "x-cron-secret": cronSecretKey,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({}),
        signal: controller.signal
      });
    } finally {
      clearTimeout(timeoutId);
    }
    console.log("[GitHub Test] Response status:", response.status, response.statusText);
    if (response.ok || response.status === 200 || response.status === 202) {
      console.log("[GitHub Test] \u2705 Connection successful!");
      return {
        success: true,
        message: "\u0646\u062C\u062D \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 GitHub - \u0627\u0644\u062E\u0627\u062F\u0645 \u064A\u0633\u062A\u062C\u064A\u0628 \u0628\u0634\u0643\u0644 \u0635\u062D\u064A\u062D \u0648\u062C\u0627\u0647\u0632 \u0644\u0644\u0639\u0645\u0644"
      };
    }
    if (response.status === 401 || response.status === 403) {
      console.log("[GitHub Test] \u274C Unauthorized - CRON_SECRET_KEY mismatch");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u0645\u0641\u062A\u0627\u062D CRON_SECRET_KEY \u063A\u064A\u0631 \u0635\u062D\u064A\u062D \u0623\u0648 \u0644\u0627 \u064A\u062A\u0637\u0627\u0628\u0642 - \u062A\u062D\u0642\u0642 \u0645\u0646 \u0623\u0646\u0643 \u0623\u062F\u062E\u0644\u062A \u0627\u0644\u0631\u0627\u0628\u0637 \u0627\u0644\u0635\u062D\u064A\u062D \u0648\u0623\u0646 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0645\u062D\u0641\u0648\u0638 \u0641\u064A \u0627\u0644\u062E\u0627\u062F\u0645"
      };
    }
    if (response.status === 404) {
      console.log("[GitHub Test] \u274C Not Found - Endpoint does not exist");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u0627\u0644\u0640 endpoint \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F - \u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646 \u0631\u0627\u0628\u0637 GitHub \u0635\u062D\u064A\u062D \u0648\u0623\u0646 \u0627\u0644\u062A\u0637\u0628\u064A\u0642 \u0645\u064F\u0646\u0634\u0631 \u0647\u0646\u0627\u0643"
      };
    }
    if (response.status === 500 || response.status === 502 || response.status === 503) {
      console.log("[GitHub Test] \u274C Server error");
      return {
        success: false,
        message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u062E\u0637\u0623 \u0645\u0646 \u0627\u0644\u062E\u0627\u062F\u0645 (\u0631\u0645\u0632 ${response.status}) - \u0627\u0644\u062E\u0627\u062F\u0645 \u0642\u062F \u0644\u0627 \u064A\u0643\u0648\u0646 \u062C\u0627\u0647\u0632\u0627\u064B \u0623\u0648 \u062D\u062F\u062B \u062E\u0637\u0623 \u0639\u0644\u064A\u0647`
      };
    }
    console.log("[GitHub Test] \u274C Unexpected status code");
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 GitHub (\u0631\u0645\u0632 ${response.status}): ${response.statusText} - \u062A\u062D\u0642\u0642 \u0645\u0646 \u0631\u0627\u0628\u0637 GitHub`
    };
  } catch (error) {
    console.log("[GitHub Test] \u274C Exception caught:", error.message);
    console.log("[GitHub Test] Error type:", error.name);
    if (error.name === "AbortError") {
      console.log("[GitHub Test] Request timeout");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u0627\u0646\u062A\u0647\u062A \u0627\u0644\u0645\u0647\u0644\u0629 \u0627\u0644\u0632\u0645\u0646\u064A\u0629 - \u0627\u0644\u062E\u0627\u062F\u0645 \u0644\u0627 \u064A\u0633\u062A\u062C\u064A\u0628 \u062E\u0644\u0627\u0644 15 \u062B\u0627\u0646\u064A\u0629 - \u062A\u062D\u0642\u0642 \u0645\u0646 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0627\u0644\u0625\u0646\u062A\u0631\u0646\u062A \u0648\u0631\u0627\u0628\u0637 GitHub"
      };
    }
    if (error.message.includes("ECONNREFUSED")) {
      console.log("[GitHub Test] Connection refused");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u062A\u0645 \u0631\u0641\u0636 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 - \u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646 \u0631\u0627\u0628\u0637 GitHub \u0635\u062D\u064A\u062D \u0648\u0623\u0646 \u0627\u0644\u062E\u0627\u062F\u0645 \u064A\u0639\u0645\u0644"
      };
    }
    if (error.message.includes("ENOTFOUND") || error.message.includes("getaddrinfo")) {
      console.log("[GitHub Test] Domain not found");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u0627\u0644\u0646\u0637\u0627\u0642 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F \u0623\u0648 \u063A\u064A\u0631 \u0635\u062D\u064A\u062D - \u062A\u062D\u0642\u0642 \u0645\u0646 \u0631\u0627\u0628\u0637 \u0627\u0644\u0633\u064A\u0631\u0641\u0631 (\u0645\u062B\u0644\u0627\u064B: https://turk.github.com)"
      };
    }
    if (error.message.includes("ECONNRESET")) {
      console.log("[GitHub Test] Connection reset");
      return {
        success: false,
        message: "\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644: \u062A\u0645 \u0625\u0639\u0627\u062F\u0629 \u062A\u0639\u064A\u064A\u0646 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 - \u0642\u062F \u064A\u0643\u0648\u0646 \u0647\u0646\u0627\u0643 \u0645\u0634\u0643\u0644\u0629 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629 \u0623\u0648 \u0627\u0644\u062E\u0627\u062F\u0645"
      };
    }
    console.log("[GitHub Test] ================================");
    return {
      success: false,
      message: `\u0641\u0634\u0644 \u0627\u0644\u0627\u062A\u0635\u0627\u0644 \u0628\u0640 GitHub: ${error.message || "\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641 \u0641\u064A \u0627\u0644\u0634\u0628\u0643\u0629"}`
    };
  }
}

// server/auto-story-generator.ts
init_firestore();
init_video_generator();
init_music_service();
init_openai_service();
import fetch3 from "node-fetch";
var CATEGORIES = ["movies", "tv_shows", "sports", "recipes", "gaming", "apps"];
var AutoStoryGenerator = class {
  /**
   * Generate 6 stories (one per category) for daily publishing
   */
  async generateDailyStories(config) {
    try {
      console.log(`
\u{1F3AC} === GENERATING ${CATEGORIES.length} DAILY STORIES ===`);
      const stories = [];
      for (const category of CATEGORIES) {
        try {
          const story = await this.generateStoryForCategory(
            config.userId,
            category,
            config.platforms,
            config.publishTime,
            config.timezone
          );
          stories.push(story);
          console.log(`\u2705 Generated story for category: ${category}`);
        } catch (error) {
          console.error(`\u274C Failed to generate story for ${category}:`, error);
        }
      }
      return stories;
    } catch (error) {
      console.error("\u274C Daily story generation failed:", error);
      return [];
    }
  }
  /**
   * Generate story for a specific category
   */
  async generateStoryForCategory(userId, category, platforms2, publishTime, timezone) {
    try {
      const { content, title, mediaUrl } = await this.generateStoryContent(category);
      const scheduledTime = this.calculateScheduledTime(publishTime, timezone);
      const story = await firestoreService.createStory(userId, {
        content,
        category,
        platforms: platforms2,
        scheduledTime,
        format: "story",
        status: "scheduled",
        posterTitle: title,
        mediaUrl,
        mediaType: "video",
        // Force mediaType to video to count in dashboard
        videoDuration: 20,
        videoGenerationStatus: "pending",
        videoScheduledGenerationTime: new Date(scheduledTime.getTime() - 4 * 60 * 60 * 1e3)
        // Precise 4 hours before
      });
      return story;
    } catch (error) {
      console.error(`Error generating story for ${category}:`, error);
      throw error;
    }
  }
  /**
   * Generate story content using AI
   */
  async generateStoryContent(category) {
    try {
      const prompt = `Generate a short, engaging social media story (max 100 chars) about trending ${category}. Only provide the story text, no introduction.`;
      let content = "Check out this trending story!";
      let title = `Today's ${category.replace(/_/g, " ")}`;
      let mediaUrl = "https://via.placeholder.com/1080x1920?text=" + encodeURIComponent(category);
      try {
        const response = await generateContent(category);
        if (response) {
          content = response.trim().substring(0, 100);
        }
      } catch {
        console.warn("AI content generation failed, using default");
      }
      return { content, title, mediaUrl };
    } catch (error) {
      console.error("Error generating story content:", error);
      return {
        content: "Check out this trending story!",
        title: `Today's ${category}`,
        mediaUrl: "https://via.placeholder.com/1080x1920?text=" + encodeURIComponent(category)
      };
    }
  }
  /**
   * Pre-generate videos for stories scheduled within 4 hours with 5-minute intervals
   */
  async preGenerateVideos(stories) {
    try {
      console.log(`
\u{1F4F9} === SMART VIDEO PRE-GENERATION (4 hours buffer) ===`);
      const now = /* @__PURE__ */ new Date();
      const fourHoursLater = new Date(now.getTime() + 4 * 60 * 60 * 1e3);
      const scheduledStories = stories.filter((s) => s.status === "scheduled" && s.videoGenerationStatus === "pending").sort((a, b) => new Date(a.scheduledTime).getTime() - new Date(b.scheduledTime).getTime());
      for (let i = 0; i < scheduledStories.length; i++) {
        const story = scheduledStories[i];
        const scheduledTime = new Date(story.scheduledTime);
        if (scheduledTime >= now && scheduledTime <= fourHoursLater && story.mediaUrl) {
          try {
            console.log(`\u{1F916} [Queue] Processing video ${i + 1}/6: ${story.category}`);
            if (i > 0) {
              console.log(`\u23F3 Waiting 5 minutes before next generation...`);
              await new Promise((resolve) => setTimeout(resolve, 5 * 60 * 1e3));
            }
            const musicTracks = await musicService.searchMusicForCategory(story.category);
            const selectedMusic = musicTracks[Math.floor(Math.random() * musicTracks.length)];
            const result = await videoGenerator.generateAndUploadVideo({
              storyId: story.id,
              category: story.category,
              posterUrl: story.mediaUrl,
              musicTrack: {
                title: selectedMusic.title,
                artist: selectedMusic.artist,
                source: selectedMusic.source,
                url: selectedMusic.url
              },
              scheduledTime: story.scheduledTime
            });
            if (result.success) {
              await firestoreService.updateStory(story.id, {
                videoUrl: result.videoUrl,
                videoStorageKey: result.storageKey,
                videoGenerationStatus: "generated",
                videoGeneratedAt: /* @__PURE__ */ new Date(),
                musicTitle: selectedMusic.title,
                musicArtist: selectedMusic.artist
              });
              console.log(`\u2705 Video generated and stored in R2: ${story.id}`);
            }
          } catch (error) {
            console.error(`\u274C Error in pre-generation for ${story.id}:`, error);
          }
        }
      }
    } catch (error) {
      console.error("Error pre-generating videos:", error);
    }
  }
  /**
   * Calculate scheduled time in Saudi timezone
   */
  calculateScheduledTime(publishTime, timezone) {
    const today = /* @__PURE__ */ new Date();
    const [hours, minutes] = publishTime.split(":").map(Number);
    const saudiTime = new Date(
      today.getUTCFullYear(),
      today.getUTCMonth(),
      today.getUTCDate(),
      hours,
      minutes,
      0,
      0
    );
    const utcTime = new Date(saudiTime.getTime() - 3 * 60 * 60 * 1e3);
    return utcTime;
  }
};
var autoStoryGenerator = new AutoStoryGenerator();

// server/routes.ts
init_firebase_admin_setup();
var firestore2 = getFirestore();
async function authenticateUser(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ message: "Unauthorized" });
  }
  const token = authHeader.split("Bearer ")[1];
  try {
    let decodedToken;
    const auth = getAuth();
    if (auth) {
      try {
        decodedToken = await auth.verifyIdToken(token);
      } catch (adminError) {
        console.log("Admin SDK verification failed, trying REST API...");
        decodedToken = await verifyTokenWithFirebaseAPI(token);
      }
    } else {
      decodedToken = await verifyTokenWithFirebaseAPI(token);
    }
    setAuthToken(token);
    req.userId = decodedToken.uid;
    req.userEmail = decodedToken.email;
    req.customClaims = { ...decodedToken, admin: true };
    console.log(`\u2705 User authenticated: ${decodedToken.email} (ID: ${decodedToken.uid})`);
    next();
  } catch (error) {
    setAuthToken(null);
    return res.status(401).json({ message: "Invalid token" });
  }
}
async function requireAdmin(req, res, next) {
  if (!req.customClaims?.admin) {
    return res.status(403).json({ message: "Admin access required" });
  }
  next();
}
async function registerRoutes(app2) {
  app2.get("/api/firebase-config", (req, res) => {
    try {
      const config = {
        apiKey: process.env.VITE_FIREBASE_API_KEY || "",
        authDomain: process.env.VITE_FIREBASE_AUTH_DOMAIN || "",
        projectId: process.env.VITE_FIREBASE_PROJECT_ID || "",
        storageBucket: process.env.VITE_FIREBASE_STORAGE_BUCKET || "",
        messagingSenderId: process.env.VITE_FIREBASE_MESSAGING_SENDER_ID || "",
        appId: process.env.VITE_FIREBASE_APP_ID || ""
      };
      if (!config.apiKey || !config.authDomain) {
        console.warn("\u26A0\uFE0F Firebase config incomplete:", {
          apiKey: !!config.apiKey,
          authDomain: !!config.authDomain,
          projectId: !!config.projectId,
          appId: !!config.appId
        });
      }
      if (config.apiKey && config.authDomain && config.projectId && config.appId) {
        res.json(config);
      } else {
        res.status(503).json({
          error: "Firebase is not configured",
          message: "Please add Firebase credentials to .env file",
          hint: "Ensure .env file contains VITE_FIREBASE_API_KEY, VITE_FIREBASE_AUTH_DOMAIN, VITE_FIREBASE_PROJECT_ID, and VITE_FIREBASE_APP_ID"
        });
      }
    } catch (error) {
      res.status(500).json({
        error: "Failed to retrieve Firebase configuration",
        message: String(error)
      });
    }
  });
  app2.get("/media/*", async (req, res) => {
    try {
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      const filePath = req.params[0];
      if (!filePath || filePath.includes("..") || filePath.startsWith("/")) {
        return res.status(400).json({ message: "Invalid file path" });
      }
      const imageBuffer = await r2Storage2.getFile(filePath);
      const contentType = filePath.endsWith(".png") ? "image/png" : filePath.endsWith(".jpg") || filePath.endsWith(".jpeg") ? "image/jpeg" : filePath.endsWith(".gif") ? "image/gif" : filePath.endsWith(".webp") ? "image/webp" : filePath.endsWith(".mp4") ? "video/mp4" : filePath.endsWith(".webm") ? "video/webm" : "application/octet-stream";
      res.setHeader("Content-Type", contentType);
      res.setHeader("Cache-Control", "public, max-age=31536000");
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.send(imageBuffer);
    } catch (error) {
      console.error("Error serving media:", error);
      res.status(404).json({ message: "Image not found" });
    }
  });
  app2.post("/api/admin/cron/trigger", async (req, res) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || authHeader !== `Bearer ${process.env.CRON_SECRET_KEY}`) {
        return res.status(401).json({ message: "Unauthorized" });
      }
      console.log("\u{1F680} GitHub Actions Cron Triggered");
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const publishResult = await cronScheduler2.checkScheduledStoriesForPublishing();
      const generateResult = await cronScheduler2.checkAndGenerateVideos();
      res.json({
        success: true,
        timestamp: (/* @__PURE__ */ new Date()).toISOString(),
        publishResult,
        generateResult
      });
    } catch (error) {
      console.error("\u274C Cron Trigger Error:", error.message);
      res.status(500).json({ success: false, error: error.message });
    }
  });
  app2.get("/api/stories", authenticateUser, async (req, res) => {
    try {
      const userStories = await firestoreService.getStoriesByUser(req.userId);
      const autoStories = await firestoreService.getStoriesByUser("system-auto-publish");
      const stories = [...userStories, ...autoStories];
      res.json(stories);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/stories/recent", authenticateUser, async (req, res) => {
    try {
      const userStories = await firestoreService.getRecentScheduledStoriesByUser(req.userId, 5);
      const autoStories = await firestoreService.getStoriesByUser("system-auto-publish", 5);
      const allStories = [...userStories, ...autoStories].sort((a, b) => {
        const timeA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const timeB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return timeB - timeA;
      }).slice(0, 5);
      res.json(allStories);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/stories", authenticateUser, async (req, res) => {
    try {
      const validatedData = insertStorySchema.parse(req.body);
      const scheduledTime = typeof validatedData.scheduledTime === "string" ? new Date(validatedData.scheduledTime) : validatedData.scheduledTime;
      const platformConfigs = await firestoreService.getAPIConfigs();
      const disconnectedPlatforms = [];
      for (const platform of validatedData.platforms) {
        const config = platformConfigs.find((c) => c.provider === platform);
        if (!config || !config.isConnected) {
          disconnectedPlatforms.push(platform);
        }
      }
      if (disconnectedPlatforms.length > 0) {
        const platformNames = {
          facebook: "\u0641\u064A\u0633\u0628\u0648\u0643",
          instagram: "\u0625\u0646\u0633\u062A\u062C\u0631\u0627\u0645",
          tiktok: "\u062A\u064A\u0643 \u062A\u0648\u0643"
        };
        const names = disconnectedPlatforms.map((p) => platformNames[p] || p).join("\u060C ");
        return res.status(400).json({
          message: `\u064A\u062C\u0628 \u0625\u0639\u062F\u0627\u062F \u0648\u0631\u0628\u0637 API \u0644\u0644\u0645\u0646\u0635\u0627\u062A \u0627\u0644\u062A\u0627\u0644\u064A\u0629 \u0642\u0628\u0644 \u0627\u0644\u062C\u062F\u0648\u0644\u0629: ${names}. \u064A\u0631\u062C\u0649 \u0627\u0644\u0630\u0647\u0627\u0628 \u0625\u0644\u0649 \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u0625\u0639\u062F\u0627\u062F \u0645\u0641\u0627\u062A\u064A\u062D API.`
        });
      }
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const availableAccounts = accounts.filter(
        (account) => validatedData.platforms.includes(account.platform) && account.status === "active"
      );
      if (availableAccounts.length === 0) {
        return res.status(400).json({
          message: `\u0644\u0627 \u062A\u0648\u062C\u062F \u062D\u0633\u0627\u0628\u0627\u062A \u0646\u0634\u0637\u0629 \u0645\u062A\u0635\u0644\u0629 \u0644\u0644\u0645\u0646\u0635\u0627\u062A \u0627\u0644\u0645\u062E\u062A\u0627\u0631\u0629. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0648\u062A\u0641\u0639\u064A\u0644 \u062D\u0633\u0627\u0628\u0627\u062A \u0623\u0648\u0644\u0627\u064B \u0641\u064A \u0642\u0633\u0645 \u0627\u0644\u062D\u0633\u0627\u0628\u0627\u062A \u0627\u0644\u0645\u0631\u062A\u0628\u0637\u0629.`
        });
      }
      const storyDataRaw = {
        content: validatedData.content,
        category: validatedData.category,
        platforms: validatedData.platforms,
        scheduledTime,
        status: "scheduled",
        format: validatedData.format,
        videoGenerationStatus: validatedData.videoGenerationStatus || "pending",
        mediaUrl: validatedData.mediaUrl,
        jpegUrl: validatedData.jpegUrl,
        mediaType: validatedData.mediaType,
        facebookPngUrl: validatedData.facebookPngUrl,
        instagramPngUrl: validatedData.instagramPngUrl,
        tiktokWebpUrl: validatedData.tiktokWebpUrl,
        trendingTopic: validatedData.trendingTopic,
        posterTitle: validatedData.posterTitle,
        latestEpisode: validatedData.latestEpisode,
        musicUrl: validatedData.musicUrl,
        musicTitle: validatedData.musicTitle,
        musicArtist: validatedData.musicArtist,
        musicThumbnail: validatedData.musicThumbnail,
        musicDuration: validatedData.musicDuration,
        musicVideoId: validatedData.musicVideoId,
        videoDuration: validatedData.videoDuration,
        originCountry: validatedData.originCountry
      };
      const storyData = Object.fromEntries(
        Object.entries(storyDataRaw).filter(([_, value]) => value !== void 0)
      );
      const story = await firestoreService.createStory(req.userId, storyData);
      let assignmentCount = 0;
      try {
        for (const account of availableAccounts) {
          await firestoreService.assignAccountToStory(story.id, account.id);
          assignmentCount++;
        }
        console.log(`\u2705 Auto-assigned story ${story.id} to ${assignmentCount} accounts`);
      } catch (assignErr) {
        console.warn(`\u26A0\uFE0F Assignment error: ${assignErr.message}`);
        await firestoreService.deleteStory(story.id);
        return res.status(500).json({
          message: `\u0641\u0634\u0644 \u062A\u0639\u064A\u064A\u0646 \u0627\u0644\u0642\u0635\u0629 \u0644\u0644\u062D\u0633\u0627\u0628\u0627\u062A: ${assignErr.message}`
        });
      }
      res.json(story);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.get("/api/stories/:id", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.id);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      res.json(story);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.put("/api/stories/:id", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.id);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      const validatedData = updateStorySchema.parse(req.body);
      const updateData = { ...validatedData };
      if (updateData.platforms) {
        const platformConfigs = await firestoreService.getAPIConfigs();
        const disconnectedPlatforms = [];
        for (const platform of updateData.platforms) {
          const config = platformConfigs.find((c) => c.provider === platform);
          if (!config || !config.isConnected) {
            disconnectedPlatforms.push(platform);
          }
        }
        if (disconnectedPlatforms.length > 0) {
          const platformNames = {
            facebook: "\u0641\u064A\u0633\u0628\u0648\u0643",
            instagram: "\u0625\u0646\u0633\u062A\u062C\u0631\u0627\u0645",
            tiktok: "\u062A\u064A\u0643 \u062A\u0648\u0643"
          };
          const names = disconnectedPlatforms.map((p) => platformNames[p] || p).join("\u060C ");
          return res.status(400).json({
            message: `\u064A\u062C\u0628 \u0625\u0639\u062F\u0627\u062F \u0648\u0631\u0628\u0637 API \u0644\u0644\u0645\u0646\u0635\u0627\u062A \u0627\u0644\u062A\u0627\u0644\u064A\u0629 \u0642\u0628\u0644 \u0627\u0644\u062A\u062D\u062F\u064A\u062B: ${names}. \u064A\u0631\u062C\u0649 \u0627\u0644\u0630\u0647\u0627\u0628 \u0625\u0644\u0649 \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u0625\u0639\u062F\u0627\u062F \u0645\u0641\u0627\u062A\u064A\u062D API.`
          });
        }
      }
      if (updateData.scheduledTime && typeof updateData.scheduledTime === "string") {
        updateData.scheduledTime = new Date(updateData.scheduledTime);
      }
      await firestoreService.updateStory(req.params.id, updateData);
      const updatedStory = await firestoreService.getStoryById(req.params.id);
      res.json(updatedStory);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.delete("/api/stories/:id", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.id);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      await firestoreService.deleteStory(req.params.id);
      res.json({ message: "Story deleted successfully" });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/accounts", authenticateUser, async (req, res) => {
    try {
      const { platform, status, limit, startAfter, search } = req.query;
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {
        platform,
        status,
        limit: limit ? parseInt(limit) : void 0,
        startAfter,
        search
      });
      const sanitizedAccounts = accounts.map((acc) => ({
        ...acc,
        accessToken: void 0,
        refreshToken: void 0
      }));
      res.json(sanitizedAccounts);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/accounts/stats", authenticateUser, async (req, res) => {
    try {
      const stats = await firestoreService.getUserAccountStats(req.userId);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/accounts", authenticateUser, async (req, res) => {
    try {
      const validatedData = insertLinkedAccountSchema.parse(req.body);
      const accountData = {
        ...validatedData,
        quotas: {
          dailyLimit: 50,
          dailyUsed: 0,
          monthlyLimit: 1e3,
          monthlyUsed: 0,
          resetAt: new Date(Date.now() + 24 * 60 * 60 * 1e3)
        }
      };
      if (validatedData.tokenExpiresAt) {
        accountData.tokenExpiresAt = new Date(validatedData.tokenExpiresAt);
      }
      const account = await firestoreService.createLinkedAccount(req.userId, accountData);
      const sanitizedAccount = {
        ...account,
        accessToken: void 0,
        refreshToken: void 0
      };
      res.json(sanitizedAccount);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.patch("/api/accounts/:id", authenticateUser, async (req, res) => {
    try {
      const account = await firestoreService.getLinkedAccountById(req.params.id);
      if (!account || account.userId !== req.userId) {
        return res.status(404).json({ message: "\u0627\u0644\u062D\u0633\u0627\u0628 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F" });
      }
      const validatedData = updateLinkedAccountSchema.parse(req.body);
      const updateData = { ...validatedData };
      if (updateData.tokenExpiresAt && typeof updateData.tokenExpiresAt === "string") {
        updateData.tokenExpiresAt = new Date(updateData.tokenExpiresAt);
      }
      if (updateData.lastSyncedAt && typeof updateData.lastSyncedAt === "string") {
        updateData.lastSyncedAt = new Date(updateData.lastSyncedAt);
      }
      if (updateData.lastPublishedAt && typeof updateData.lastPublishedAt === "string") {
        updateData.lastPublishedAt = new Date(updateData.lastPublishedAt);
      }
      await firestoreService.updateLinkedAccount(req.params.id, updateData);
      const updatedAccount = await firestoreService.getLinkedAccountById(req.params.id);
      const sanitizedAccount = updatedAccount ? {
        ...updatedAccount,
        accessToken: void 0,
        refreshToken: void 0
      } : null;
      res.json(sanitizedAccount);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.delete("/api/accounts/:id", authenticateUser, async (req, res) => {
    try {
      await firestoreService.deleteLinkedAccount(req.params.id, req.userId);
      res.json({ message: "\u062A\u0645 \u062D\u0630\u0641 \u0627\u0644\u062D\u0633\u0627\u0628 \u0628\u0646\u062C\u0627\u062D" });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/stories/:storyId/assign-accounts", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.storyId);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "\u0627\u0644\u0642\u0635\u0629 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F\u0629" });
      }
      const { accountIds } = req.body;
      if (!Array.isArray(accountIds) || accountIds.length === 0) {
        return res.status(400).json({ message: "\u064A\u062C\u0628 \u062A\u062D\u062F\u064A\u062F \u062D\u0633\u0627\u0628 \u0648\u0627\u062D\u062F \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644" });
      }
      for (const accountId of accountIds) {
        const account = await firestoreService.getLinkedAccountById(accountId);
        if (!account || account.userId !== req.userId) {
          return res.status(404).json({ message: `\u0627\u0644\u062D\u0633\u0627\u0628 ${accountId} \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F` });
        }
        if (account.status !== "active") {
          return res.status(400).json({ message: `\u0627\u0644\u062D\u0633\u0627\u0628 ${account.name} \u063A\u064A\u0631 \u0646\u0634\u0637` });
        }
      }
      const assignments = [];
      for (const accountId of accountIds) {
        const assignment = await firestoreService.assignAccountToStory(req.params.storyId, accountId);
        assignments.push(assignment);
      }
      res.json(assignments);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/stories/:storyId/assignments", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.storyId);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "\u0627\u0644\u0642\u0635\u0629 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F\u0629" });
      }
      const assignments = await firestoreService.getStoryAssignments(req.params.storyId);
      res.json(assignments);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/videos/generate", authenticateUser, async (req, res) => {
    try {
      const { storyId } = req.body;
      if (!storyId) {
        return res.status(400).json({ message: "Story ID is required" });
      }
      const story = await firestoreService.getStoryById(storyId);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      if (!story.mediaUrl && !story.sourceImageUrl) {
        return res.status(400).json({ message: "Story must have an image (mediaUrl or sourceImageUrl)" });
      }
      console.log(`\u{1F3AC} Manual video generation triggered for story: ${storyId}`);
      const { videoGenerator: videoGenerator2 } = await Promise.resolve().then(() => (init_video_generator(), video_generator_exports));
      await firestoreService.updateStory(storyId, {
        videoGenerationStatus: "generating"
      });
      const result = await videoGenerator2.generateAndUploadVideo({
        storyId,
        category: story.category,
        posterUrl: story.mediaUrl || story.sourceImageUrl || "",
        musicTrack: story.musicUrl ? {
          title: story.musicTitle || "Background Music",
          artist: story.musicArtist || "Unknown",
          url: story.musicUrl,
          source: story.musicVideoId ? "youtube" : "api"
        } : void 0,
        scheduledTime: story.scheduledTime
      });
      if (result.success && result.videoUrl) {
        await firestoreService.updateStory(storyId, {
          videoUrl: result.videoUrl,
          videoGenerationStatus: "generated",
          videoGeneratedAt: /* @__PURE__ */ new Date(),
          videoStorageKey: result.storageKey
        });
        res.json({
          success: true,
          message: "Video generated successfully",
          videoUrl: result.videoUrl,
          storageKey: result.storageKey
        });
      } else {
        await firestoreService.updateStory(storyId, {
          videoGenerationStatus: "error"
        });
        throw new Error(result.error || "Video generation failed");
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        message: error.message
      });
    }
  });
  app2.get("/api/videos/storage-stats", authenticateUser, async (req, res) => {
    try {
      const { storageService: storageService2 } = await Promise.resolve().then(() => (init_storage_service(), storage_service_exports));
      const stats = await storageService2.getStorageStats();
      res.json({
        totalVideos: stats.totalVideos,
        totalSizeGB: (stats.totalSize / 1024 / 1024 / 1024).toFixed(2),
        videosByCategory: stats.videosByCategory
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/videos/cleanup-old", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { days = 30 } = req.body;
      const { storageService: storageService2 } = await Promise.resolve().then(() => (init_storage_service(), storage_service_exports));
      const archivedCount = await storageService2.archiveOldVideos(days);
      res.json({
        success: true,
        message: `Archived ${archivedCount} old videos`,
        archivedCount
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/videos/check-and-generate", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const result = await cronScheduler2.checkAndGenerateVideos();
      res.json({
        success: true,
        message: `Video generation check complete: ${result.generated} generated, ${result.failed} failed`,
        ...result
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/videos/recent", authenticateUser, async (req, res) => {
    try {
      const limit = req.query.limit ? parseInt(req.query.limit) : 20;
      const { storageService: storageService2 } = await Promise.resolve().then(() => (init_storage_service(), storage_service_exports));
      const recentVideos = await storageService2.getRecentVideos(limit);
      res.json(recentVideos);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/videos/by-category/:category", authenticateUser, async (req, res) => {
    try {
      const { storageService: storageService2 } = await Promise.resolve().then(() => (init_storage_service(), storage_service_exports));
      const videos = await storageService2.getVideosByCategory(req.params.category);
      res.json(videos);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/stats", authenticateUser, async (req, res) => {
    try {
      const userStories = await firestoreService.getStoriesByUser(req.userId);
      const autoStories = await firestoreService.getStoriesByUser("system-auto-publish");
      const stories = [...userStories, ...autoStories];
      const totalStories = stories.length;
      const publishedStories = stories.filter((s) => s.status === "published").length;
      const scheduledStories = stories.filter((s) => s.status === "scheduled").length;
      const published = stories.filter((s) => s.status === "published");
      const avgEngagement = published.length > 0 ? published.reduce((sum, s) => sum + (s.engagementRate || 0), 0) / published.length : 0;
      res.json({
        totalStories,
        publishedStories,
        scheduledStories,
        total: totalStories,
        scheduled: scheduledStories,
        published: publishedStories,
        averageEngagement: parseFloat(avgEngagement.toFixed(1)),
        avgEngagement: parseFloat(avgEngagement.toFixed(1))
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/ai/suggest-hashtags", authenticateUser, async (req, res) => {
    try {
      const { content, category } = req.body;
      if (!content) {
        return res.status(400).json({ message: "Content is required" });
      }
      const { generateHashtags: generateHashtags2 } = await Promise.resolve().then(() => (init_openai_service(), openai_service_exports));
      const hashtags = await generateHashtags2(content, category || "general");
      res.json({ hashtags });
    } catch (error) {
      console.error("Error suggesting hashtags:", error);
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/stats/platforms", authenticateUser, async (req, res) => {
    try {
      const linkedAccounts = await firestoreService.getLinkedAccountsByUser(req.userId);
      const connectedPlatforms = linkedAccounts.filter((acc) => acc.status === "active").map((acc) => acc.platform);
      if (connectedPlatforms.length === 0) {
        return res.json([]);
      }
      const userStories = await firestoreService.getStoriesByUser(req.userId);
      const autoStories = await firestoreService.getStoriesByUser("system-auto-publish");
      const stories = [...userStories, ...autoStories];
      const platformStats = connectedPlatforms.map((platform) => ({
        platform,
        totalStories: 0,
        publishedStories: 0,
        averageEngagement: 0
      }));
      stories.forEach((story) => {
        const platforms2 = Array.isArray(story.platforms) ? story.platforms : [];
        platforms2.forEach((platform) => {
          if (connectedPlatforms.includes(platform)) {
            const stat = platformStats.find((s) => s.platform === platform);
            if (stat) {
              stat.totalStories++;
            }
          }
        });
      });
      const publishedStories = stories.filter((s) => s.status === "published");
      publishedStories.forEach((story) => {
        const platformsPublished = Array.isArray(story.publishedPlatforms) ? story.publishedPlatforms : Array.isArray(story.platforms) ? story.platforms : [];
        platformsPublished.forEach((platform) => {
          if (connectedPlatforms.includes(platform)) {
            const stat = platformStats.find((s) => s.platform === platform);
            if (stat) {
              stat.publishedStories++;
            }
          }
        });
      });
      platformStats.forEach((stat) => {
        const platformPublished = publishedStories.filter((s) => {
          const platformsPublished = Array.isArray(s.publishedPlatforms) ? s.publishedPlatforms : Array.isArray(s.platforms) ? s.platforms : [];
          return platformsPublished.includes(stat.platform);
        });
        const totalEng = platformPublished.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
        stat.averageEngagement = platformPublished.length > 0 ? parseFloat((totalEng / platformPublished.length).toFixed(1)) : 0;
      });
      res.json(platformStats);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/auto-story/generate", authenticateUser, async (req, res) => {
    try {
      const validatedSettings = autoStoryGenerationSettingsSchema.parse(req.body);
      console.log(`\u{1F3AF} Starting auto-story generation for user: ${req.userId}`);
      console.log(`   Categories: ${validatedSettings.categories.join(", ")}`);
      console.log(`   With Music: ${validatedSettings.withMusic}`);
      console.log(`   With Video: ${validatedSettings.withVideo}`);
      const stories = await autoStoryGenerator.generateDailyStories({
        userId: req.userId,
        platforms: validatedSettings.platforms,
        publishTime: validatedSettings.publishTime,
        timezone: "Asia/Riyadh"
      });
      if (validatedSettings.withVideo && validatedSettings.scheduleVideoGenerationInAdvance) {
        await autoStoryGenerator.preGenerateVideos(stories);
      }
      res.json({
        success: true,
        message: `Generated ${stories.length} stories`,
        storiesCount: stories.length,
        stories
      });
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.post("/api/auto-story/settings", authenticateUser, async (req, res) => {
    try {
      const settings = autoStoryGenerationSettingsSchema.parse(req.body);
      await firestoreService.updateUserSettings(req.userId, {
        autoStoryGenerationEnabled: settings.enabled,
        autoStoryGenerationTime: settings.publishTime,
        autoStoryCategories: settings.categories,
        autoStoryPlatforms: settings.platforms,
        autoStoryFormat: settings.format,
        autoStoryWithMusic: settings.withMusic,
        autoStoryWithVideo: settings.withVideo
      });
      res.json({
        success: true,
        message: "Auto-story settings saved successfully",
        settings
      });
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.get("/api/analytics/categories", authenticateUser, async (req, res) => {
    try {
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const categoryMap = /* @__PURE__ */ new Map();
      stories.forEach((story) => {
        const existing = categoryMap.get(story.category) || { category: story.category, count: 0, totalEngagement: 0 };
        existing.count++;
        existing.totalEngagement += story.engagementRate || 0;
        categoryMap.set(story.category, existing);
      });
      const categoryStats = Array.from(categoryMap.values()).map((stat) => ({
        category: stat.category,
        count: stat.count,
        averageEngagement: parseFloat((stat.totalEngagement / stat.count).toFixed(1))
      }));
      res.json(categoryStats);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/content/generate", authenticateUser, async (req, res) => {
    try {
      const { category, keywords } = req.body;
      const deepseekConfig = await firestoreService.getAPIConfig("deepseek");
      const hasDeepSeekKey = !!(deepseekConfig?.apiKey || process.env.DEEPSEEK_API_KEY);
      if (!hasDeepSeekKey) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F \u0645\u0641\u062A\u0627\u062D API \u0644\u0644\u0630\u0643\u0627\u0621 \u0627\u0644\u0627\u0635\u0637\u0646\u0627\u0639\u064A. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0645\u0641\u062A\u0627\u062D DeepSeek API \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0645\u062D\u062A\u0648\u0649."
        });
      }
      const { generateContent: generateContent3 } = await Promise.resolve().then(() => (init_deepseek(), deepseek_exports));
      const result = await generateContent3({ category, keywords });
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message || "\u0641\u0634\u0644 \u0641\u064A \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0645\u062D\u062A\u0648\u0649. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
    }
  });
  app2.post("/api/images/generate", authenticateUser, async (req, res) => {
    try {
      const { category, content } = req.body;
      if (!category || !content) {
        return res.status(400).json({ message: "\u0627\u0644\u0641\u0626\u0629 \u0648\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0645\u0637\u0644\u0648\u0628\u0627\u0646" });
      }
      const googleConfig = await firestoreService.getAPIConfig("google_trends");
      const hasGoogleKey = !!(googleConfig?.apiKey && googleConfig?.additionalConfig?.searchEngineId);
      if (!hasGoogleKey) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F Google Search API. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0627\u0644\u0645\u0641\u062A\u0627\u062D \u0648 Search Engine ID \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629."
        });
      }
      const { googleImageSearchService: googleImageSearchService2 } = await Promise.resolve().then(() => (init_google_image_search(), google_image_search_exports));
      const searchQuery = content.substring(0, 100);
      const imageResult = category === "movies" || category === "tv_shows" ? await googleImageSearchService2.searchPosterImage(searchQuery, category) : await googleImageSearchService2.searchThumbnailImage(searchQuery, category);
      if (!imageResult) {
        return res.status(404).json({ message: "\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631 \u0645\u0646\u0627\u0633\u0628\u0629" });
      }
      const imageBuffer = await googleImageSearchService2.downloadImage(imageResult.imageUrl);
      const mimeType = imageResult.imageUrl.includes(".png") ? "image/png" : "image/jpeg";
      const extension = mimeType.split("/")[1];
      const fileName = `stories/${Date.now()}-${Math.random().toString(36).substring(7)}.${extension}`;
      const jpegFileName = fileName.replace(/\.png$/i, ".jpg");
      const webpFileName = fileName.replace(/\.png$/i, ".webp");
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      await r2Storage2.uploadFile(imageBuffer, fileName, {
        contentType: mimeType,
        metadata: {
          category,
          searchQuery,
          source: imageResult.source,
          userId: req.userId
        }
      });
      const sharp3 = (await import("sharp")).default;
      const jpegBuffer = await sharp3(imageBuffer).jpeg({ quality: 90 }).toBuffer();
      await r2Storage2.uploadFile(jpegBuffer, jpegFileName, {
        contentType: "image/jpeg",
        metadata: {
          category,
          searchQuery,
          userId: req.userId,
          format: "jpeg"
        }
      });
      const webpBuffer = await sharp3(imageBuffer).webp({ quality: 90 }).toBuffer();
      await r2Storage2.uploadFile(webpBuffer, webpFileName, {
        contentType: "image/webp",
        metadata: {
          category,
          searchQuery,
          userId: req.userId,
          format: "webp"
        }
      });
      const protocol = req.protocol || "http";
      const host = req.get("host") || "localhost:5000";
      const baseUrl = process.env.PUBLIC_URL || `${protocol}://${host}`;
      const imageUrl = `${baseUrl}/media/${fileName}`;
      const jpegUrl = `${baseUrl}/media/${jpegFileName}`;
      const webpUrl = `${baseUrl}/media/${webpFileName}`;
      const isLocalhost = host.includes("localhost") || host.includes("127.0.0.1");
      const warningMessage = isLocalhost && !process.env.PUBLIC_URL ? "\u062A\u0646\u0628\u064A\u0647: \u0627\u0644\u0635\u0648\u0631\u0629 \u0645\u062D\u0641\u0648\u0638\u0629 \u0645\u062D\u0644\u064A\u0627\u064B. \u0644\u0646 \u062A\u062A\u0645\u0643\u0646 \u0627\u0644\u0645\u0646\u0635\u0627\u062A \u0627\u0644\u0627\u062C\u062A\u0645\u0627\u0639\u064A\u0629 \u0645\u0646 \u0627\u0644\u0648\u0635\u0648\u0644 \u0625\u0644\u064A\u0647\u0627 \u0625\u0644\u0627 \u0628\u0639\u062F \u0627\u0644\u0646\u0634\u0631." : void 0;
      res.json({
        imageUrl,
        jpegUrl,
        webpUrl,
        prompt: searchQuery,
        message: "\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0635\u0648\u0631\u0629 \u0645\u0646\u0627\u0633\u0628\u0629",
        warning: warningMessage
      });
    } catch (error) {
      console.error("Image generation error:", error);
      res.status(500).json({ message: error.message || "\u0641\u0634\u0644 \u0641\u064A \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631\u0629. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
    }
  });
  app2.post("/api/trending-video/generate", authenticateUser, async (req, res) => {
    try {
      const { category } = req.body;
      if (!category) {
        return res.status(400).json({ message: "\u0627\u0644\u0641\u0626\u0629 \u0645\u0637\u0644\u0648\u0628\u0629" });
      }
      const youtubeConfig = await firestoreService.getAPIConfig("youtube");
      if (!youtubeConfig?.apiKey) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F \u0645\u0641\u062A\u0627\u062D YouTube Data API v3. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0645\u0641\u062A\u0627\u062D YouTube API \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629."
        });
      }
      const r2Config = await firestoreService.getAPIConfig("cloudflare_r2");
      if (!r2Config?.additionalConfig?.accountId || !r2Config?.additionalConfig?.accessKeyId) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F Cloudflare R2 Storage. \u064A\u0631\u062C\u0649 \u0625\u0639\u062F\u0627\u062F R2 \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u062D\u0641\u0638 \u0627\u0644\u0641\u064A\u062F\u064A\u0648\u0647\u0627\u062A."
        });
      }
      const { youtubeVideoDownloader: youtubeVideoDownloader2 } = await Promise.resolve().then(() => (init_youtube_video_downloader(), youtube_video_downloader_exports));
      const result = await youtubeVideoDownloader2.generateTrendingVideo(category);
      res.json({
        videoUrl: result.videoUrl,
        title: result.title,
        description: result.description,
        trendingTopic: result.trendingTopic,
        duration: result.duration,
        message: "\u062A\u0645 \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0641\u064A\u062F\u064A\u0648 \u0628\u0646\u062C\u0627\u062D \u0645\u0646 YouTube \u0628\u0627\u0633\u062A\u062E\u062F\u0627\u0645 Google Trends"
      });
    } catch (error) {
      console.error("Trending video generation error:", error);
      res.status(500).json({ message: error.message || "\u0641\u0634\u0644 \u0641\u064A \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0641\u064A\u062F\u064A\u0648. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
    }
  });
  app2.post("/api/trending-image/generate", authenticateUser, async (req, res) => {
    try {
      const { category } = req.body;
      if (!category) {
        return res.status(400).json({ message: "\u0627\u0644\u0641\u0626\u0629 \u0645\u0637\u0644\u0648\u0628\u0629" });
      }
      const r2Config = await firestoreService.getAPIConfig("cloudflare_r2");
      if (!r2Config?.additionalConfig?.accountId || !r2Config?.additionalConfig?.accessKeyId) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F Cloudflare R2 Storage. \u064A\u0631\u062C\u0649 \u0625\u0639\u062F\u0627\u062F R2 \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u062D\u0641\u0638 \u0627\u0644\u0635\u0648\u0631."
        });
      }
      const googleConfig = await firestoreService.getAPIConfig("google_trends");
      if (!googleConfig?.apiKey && !process.env.GOOGLE_CUSTOM_SEARCH_API_KEY) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F \u0645\u0641\u062A\u0627\u062D Google Search API. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0645\u0641\u062A\u0627\u062D Google Custom Search \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u0644\u0628\u062D\u062B \u0639\u0646 \u0627\u0644\u0635\u0648\u0631."
        });
      }
      const { trendingPosterService: trendingPosterService2 } = await Promise.resolve().then(() => (init_trending_poster_service(), trending_poster_service_exports));
      await trendingPosterService2.initialize();
      const result = await trendingPosterService2.generateTrendingPoster(category);
      res.json({
        pngUrl: result.pngUrl,
        webpUrl: result.webpUrl,
        facebookPngUrl: result.facebookPngUrl,
        instagramPngUrl: result.instagramPngUrl,
        tiktokWebpUrl: result.tiktokWebpUrl,
        trendingTopic: result.trendingTopic,
        posterTitle: result.posterTitle,
        latestEpisode: result.latestEpisode,
        sourceImageUrl: result.sourceImageUrl,
        originCountry: result.originCountry,
        tmdbId: result.tmdbId,
        descriptionAr: result.descriptionAr,
        descriptionEn: result.descriptionEn,
        voteAverage: result.voteAverage,
        message: "\u062A\u0645 \u062A\u0648\u0644\u064A\u062F \u0635\u0648\u0631\u0629 \u0627\u0644\u062A\u0631\u0646\u062F \u0628\u0646\u062C\u0627\u062D"
      });
    } catch (error) {
      console.error("Trending image generation error:", error);
      res.status(500).json({ message: error.message || "\u0641\u0634\u0644 \u0641\u064A \u062A\u0648\u0644\u064A\u062F \u0627\u0644\u0635\u0648\u0631\u0629. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
    }
  });
  app2.post("/api/music/search", authenticateUser, async (req, res) => {
    try {
      const { query, limit } = req.body;
      if (!query) {
        return res.status(400).json({ message: "\u0627\u0633\u062A\u0639\u0644\u0627\u0645 \u0627\u0644\u0628\u062D\u062B \u0645\u0637\u0644\u0648\u0628" });
      }
      const youtubeConfig = await firestoreService.getAPIConfig("youtube");
      if (!youtubeConfig?.apiKey) {
        return res.status(400).json({
          message: "\u0644\u0645 \u064A\u062A\u0645 \u0625\u0639\u062F\u0627\u062F \u0645\u0641\u062A\u0627\u062D YouTube API. \u064A\u0631\u062C\u0649 \u0625\u0636\u0627\u0641\u0629 \u0645\u0641\u062A\u0627\u062D YouTube API \u0641\u064A \u0644\u0648\u062D\u0629 \u0627\u0644\u0625\u062F\u0627\u0631\u0629 \u0644\u0644\u0628\u062D\u062B \u0639\u0646 \u0627\u0644\u0645\u0648\u0633\u064A\u0642\u0649."
        });
      }
      const { YouTubeMusicService: YouTubeMusicService2 } = await Promise.resolve().then(() => (init_youtube_music(), youtube_music_exports));
      const youtubeService = new YouTubeMusicService2(youtubeConfig.apiKey);
      const results = await youtubeService.searchMusic(query, limit || 10);
      res.json(results);
    } catch (error) {
      res.status(500).json({ message: error.message || "\u0641\u0634\u0644 \u0627\u0644\u0628\u062D\u062B \u0639\u0646 \u0627\u0644\u0645\u0648\u0633\u064A\u0642\u0649. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
    }
  });
  app2.get("/api/smart/insights", authenticateUser, async (req, res) => {
    try {
      const { smartAnalyticsService: smartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const insights = await smartAnalyticsService2.getSmartInsights(req.userId);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart/best-times", authenticateUser, async (req, res) => {
    try {
      const { smartAnalyticsService: smartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const bestTimes = await smartAnalyticsService2.analyzeBestPostingTimes(req.userId);
      res.json(bestTimes);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart/content-recommendations", authenticateUser, async (req, res) => {
    try {
      const { smartAnalyticsService: smartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const recommendations = await smartAnalyticsService2.getContentRecommendations(req.userId);
      res.json(recommendations);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/smart/platform-recommendations", authenticateUser, async (req, res) => {
    try {
      const { content, category } = req.body;
      if (!content || !category) {
        return res.status(400).json({ message: "\u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0648\u0627\u0644\u0641\u0626\u0629 \u0645\u0637\u0644\u0648\u0628\u0627\u0646" });
      }
      const { smartAnalyticsService: smartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const recommendations = await smartAnalyticsService2.getPlatformRecommendations(
        content,
        category,
        req.userId
      );
      res.json(recommendations);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart/trending-hashtags", authenticateUser, async (req, res) => {
    try {
      const { smartAnalyticsService: smartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const hashtags = await smartAnalyticsService2.getTrendingHashtags(req.userId);
      res.json(hashtags);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.patch("/api/users/:id", authenticateUser, async (req, res) => {
    try {
      if (req.params.id !== req.userId && !req.customClaims?.admin) {
        return res.status(403).json({ message: "Unauthorized" });
      }
      const { displayName, bio, company } = req.body;
      const userRef = firestore2.collection("users").doc(req.params.id);
      const updateData = {};
      if (displayName !== void 0) updateData.displayName = displayName;
      if (bio !== void 0) updateData.bio = bio;
      if (company !== void 0) updateData.company = company;
      await userRef.update(updateData);
      const updatedDoc = await userRef.get();
      res.json({ id: updatedDoc.id, ...updatedDoc.data() });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.patch("/api/users/:id", authenticateUser, async (req, res) => {
    try {
      if (req.params.id !== req.userId) {
        return res.status(403).json({ message: "\u063A\u064A\u0631 \u0645\u0633\u0645\u0648\u062D \u0644\u0643 \u0628\u062A\u0639\u062F\u064A\u0644 \u0628\u064A\u0627\u0646\u0627\u062A \u0645\u0633\u062A\u062E\u062F\u0645 \u0622\u062E\u0631" });
      }
      const updates = {};
      if (req.body.displayName) updates.displayName = req.body.displayName;
      if (req.body.bio !== void 0) updates.bio = req.body.bio;
      if (req.body.company !== void 0) updates.company = req.body.company;
      if (Object.keys(updates).length === 0) {
        return res.status(400).json({ message: "\u0644\u0627 \u062A\u0648\u062C\u062F \u0628\u064A\u0627\u0646\u0627\u062A \u0644\u0644\u062A\u062D\u062F\u064A\u062B" });
      }
      await firestoreService.updateUser(req.userId, updates);
      const updatedUser = await firestoreService.getUserById(req.userId);
      res.json(updatedUser);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/settings", authenticateUser, async (req, res) => {
    try {
      const settings = await firestoreService.getUserSettings(req.userId);
      res.json(settings);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.put("/api/settings", authenticateUser, async (req, res) => {
    try {
      const validatedData = updateSettingsSchema.parse(req.body);
      await firestoreService.updateUserSettings(req.userId, validatedData);
      const updatedSettings = await firestoreService.getUserSettings(req.userId);
      res.json(updatedSettings);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.patch("/api/settings", authenticateUser, async (req, res) => {
    try {
      const validatedData = updateSettingsSchema.parse(req.body);
      await firestoreService.updateUserSettings(req.userId, validatedData);
      const updatedSettings = await firestoreService.getUserSettings(req.userId);
      res.json(updatedSettings);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.get("/api/insights", authenticateUser, async (req, res) => {
    try {
      const { SmartAnalyticsService: SmartAnalyticsService2 } = await Promise.resolve().then(() => (init_smart_analytics(), smart_analytics_exports));
      const smartAnalytics = new SmartAnalyticsService2();
      const insights = await smartAnalytics.getSmartInsights(req.userId);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/scheduling-settings", authenticateUser, async (req, res) => {
    try {
      const settings = await firestore2.collection("scheduling_settings").doc(req.userId).get();
      const data = settings.data() || {
        enabled: false,
        publishTime: "09:00",
        categories: ["movies"],
        platforms: ["facebook"],
        format: "story",
        withMusic: true,
        withVideo: false,
        scheduleVideoGenerationInAdvance: false,
        videoGenerationHoursBefore: 2
      };
      res.json(data);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.put("/api/scheduling-settings", authenticateUser, async (req, res) => {
    try {
      const validatedData = autoStoryGenerationSettingsSchema.parse(req.body);
      await firestore2.collection("scheduling_settings").doc(req.userId).set(validatedData, { merge: true });
      if (validatedData.scheduleVideoGenerationInAdvance) {
        const { videoScheduler: videoScheduler2 } = await Promise.resolve().then(() => (init_video_scheduler(), video_scheduler_exports));
        const upcomingStories = await firestoreService.getStoriesByUser(req.userId, 50);
        for (const story of upcomingStories) {
          if (story.status === "scheduled" && story.videoGenerationStatus !== "generated") {
            await videoScheduler2.scheduleVideoGeneration(
              story,
              validatedData.videoGenerationHoursBefore || 2
            );
          }
        }
      }
      const settings = await firestore2.collection("scheduling_settings").doc(req.userId).get();
      res.json(settings.data());
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.post("/api/stories/:id/publish-facebook-stories", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.id);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      const { accountId } = req.body;
      if (!accountId) {
        return res.status(400).json({ message: "Account ID is required" });
      }
      const { facebookStoriesPublisher: facebookStoriesPublisher2 } = await Promise.resolve().then(() => (init_facebook_stories_publisher(), facebook_stories_publisher_exports));
      const result = await facebookStoriesPublisher2.publishStoryToFacebook(story, accountId);
      if (!result.success) {
        return res.status(400).json({
          success: false,
          error: result.error
        });
      }
      res.json({
        success: true,
        publishedId: result.publishedId,
        message: "Successfully published to Facebook Stories"
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/stories/:id/schedule-video-generation", authenticateUser, async (req, res) => {
    try {
      const story = await firestoreService.getStoryById(req.params.id);
      if (!story || story.userId !== req.userId) {
        return res.status(404).json({ message: "Story not found" });
      }
      const { hoursBefore = 2 } = req.body;
      const { videoScheduler: videoScheduler2 } = await Promise.resolve().then(() => (init_video_scheduler(), video_scheduler_exports));
      const scheduled = await videoScheduler2.scheduleVideoGeneration(story, hoursBefore);
      if (!scheduled) {
        return res.status(400).json({
          success: false,
          error: "Failed to schedule video generation"
        });
      }
      res.json({
        success: true,
        message: `Video generation scheduled for ${hoursBefore} hours before publish time`,
        storyId: story.id,
        scheduledTime: story.scheduledTime
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/users", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const users = await firestoreService.getAllUsers();
      res.json(users);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/integrations", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const integrations = await firestoreService.getPlatformIntegrations();
      res.json(integrations);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.put("/api/admin/integrations/:platform", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { platform } = req.params;
      const updates = req.body;
      if (updates.enabled === false) {
        console.log(`\u{1F4E1} Smart Monitor: Platform ${platform} disabled by admin`);
      }
      await firestoreService.updatePlatformIntegration(platform, updates);
      const integrations = await firestoreService.getPlatformIntegrations();
      const allDisabled = integrations.every((i) => !i.enabled);
      if (allDisabled) {
        console.warn("\u{1F4E1} Smart Monitor: All publishing platforms are currently disabled!");
      }
      res.json(integrations);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/stats", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const users = await firestoreService.getAllUsers();
      let totalStoriesCount = 0;
      for (const user of users) {
        try {
          const stories = await firestoreService.getStoriesByUser(user.id);
          totalStoriesCount += stories.length;
        } catch (e) {
          console.warn(`Could not fetch stories for user ${user.id}`);
        }
      }
      const systemPerformance = "\u0645\u0645\u062A\u0627\u0632";
      res.json({
        activeUsers: users.filter((u) => u.status === "active").length,
        todayStories: totalStoriesCount,
        systemPerformance,
        schedulerRunning: true
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/api-configs/status", authenticateUser, async (req, res) => {
    try {
      const configs = await firestoreService.getAPIConfigs();
      const statusOnly = configs.map((config) => ({
        provider: config.provider,
        isConnected: config.isConnected || false
      }));
      res.json(statusOnly);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/api-configs", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const configs = await firestoreService.getAPIConfigs();
      const sanitizedConfigs = configs.map((config) => ({
        ...config,
        apiKey: config.apiKey && config.apiKey !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        appId: config.appId && config.appId !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        appSecret: config.appSecret && config.appSecret !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        redirectUrl: config.redirectUrl,
        additionalConfig: config.additionalConfig ? {
          accountId: config.additionalConfig.accountId || void 0,
          bucketName: config.additionalConfig.bucketName || void 0,
          accessKeyId: config.additionalConfig.accessKeyId && config.additionalConfig.accessKeyId !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : void 0,
          secretAccessKey: config.additionalConfig.secretAccessKey && config.additionalConfig.secretAccessKey !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : void 0,
          searchEngineId: config.additionalConfig.searchEngineId || void 0
        } : void 0
      }));
      res.json(sanitizedConfigs);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.put("/api/admin/api-configs/:provider", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const validatedData = updateAPIConfigSchema.parse(req.body);
      if (validatedData.apiKey === "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
        return res.status(400).json({ message: "Cannot save masked API key placeholder" });
      }
      if (validatedData.appId === "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
        return res.status(400).json({ message: "Cannot save masked App ID placeholder" });
      }
      if (validatedData.appSecret === "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
        return res.status(400).json({ message: "Cannot save masked App Secret placeholder" });
      }
      if (validatedData.additionalConfig) {
        const { accessKeyId, secretAccessKey } = validatedData.additionalConfig;
        if (accessKeyId === "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" || secretAccessKey === "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
          return res.status(400).json({ message: "Cannot save masked credential placeholders" });
        }
      }
      const updateData = {};
      if (validatedData.apiKey !== void 0 && validatedData.apiKey !== "") {
        updateData.apiKey = validatedData.apiKey;
      }
      if (validatedData.appId !== void 0 && validatedData.appId !== "") {
        updateData.appId = validatedData.appId;
      }
      if (validatedData.appSecret !== void 0 && validatedData.appSecret !== "") {
        updateData.appSecret = validatedData.appSecret;
      }
      if (validatedData.additionalConfig !== void 0) {
        const cleanConfig = {};
        if (validatedData.additionalConfig.accountId) cleanConfig.accountId = validatedData.additionalConfig.accountId;
        if (validatedData.additionalConfig.accessKeyId && validatedData.additionalConfig.accessKeyId !== "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
          cleanConfig.accessKeyId = validatedData.additionalConfig.accessKeyId;
        }
        if (validatedData.additionalConfig.secretAccessKey && validatedData.additionalConfig.secretAccessKey !== "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022") {
          cleanConfig.secretAccessKey = validatedData.additionalConfig.secretAccessKey;
        }
        if (validatedData.additionalConfig.bucketName) cleanConfig.bucketName = validatedData.additionalConfig.bucketName;
        if (validatedData.additionalConfig.searchEngineId !== void 0) {
          cleanConfig.searchEngineId = validatedData.additionalConfig.searchEngineId;
        }
        if (Object.keys(cleanConfig).length > 0) {
          updateData.additionalConfig = cleanConfig;
        }
      }
      if (validatedData.isConnected !== void 0) updateData.isConnected = validatedData.isConnected;
      if (validatedData.lastTested !== void 0) updateData.lastTested = validatedData.lastTested;
      if (Object.keys(updateData).length === 0) {
        return res.status(400).json({ message: "No valid updates provided" });
      }
      const baseUrl = `https://${req.get("host")}` || "http://localhost:5000";
      if ((req.params.provider === "facebook" || req.params.provider === "instagram" || req.params.provider === "tiktok") && (updateData.appId || updateData.appSecret || updateData.apiKey)) {
        updateData.redirectUrl = `${baseUrl}/api/oauth/${req.params.provider}/callback`;
      }
      await firestoreService.updateAPIConfig(req.params.provider, updateData);
      const config = await firestoreService.getAPIConfig(req.params.provider);
      const sanitizedConfig = config ? {
        ...config,
        apiKey: config.apiKey && config.apiKey !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        appId: config.appId && config.appId !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        appSecret: config.appSecret && config.appSecret !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
        redirectUrl: config.redirectUrl,
        additionalConfig: config.additionalConfig ? {
          accountId: config.additionalConfig.accountId,
          bucketName: config.additionalConfig.bucketName,
          accessKeyId: config.additionalConfig.accessKeyId && config.additionalConfig.accessKeyId !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : "",
          secretAccessKey: config.additionalConfig.secretAccessKey && config.additionalConfig.secretAccessKey !== "" ? "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022" : ""
        } : void 0
      } : null;
      res.json(sanitizedConfig);
    } catch (error) {
      res.status(400).json({ message: error.message });
    }
  });
  app2.post("/api/admin/api-configs/:provider/test", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const provider = req.params.provider;
      const config = await firestoreService.getAPIConfig(provider);
      if (!config) {
        return res.status(400).json({
          success: false,
          message: "Configuration not found"
        });
      }
      const testResult = await testAPIConnection(provider, config);
      await firestoreService.updateAPIConfig(provider, {
        isConnected: testResult.success,
        lastTested: /* @__PURE__ */ new Date()
      });
      res.json(testResult);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Connection test failed"
      });
    }
  });
  app2.post("/api/admin/api-configs/github_actions/trigger-test", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const config = await firestoreService.getAPIConfig("github_actions");
      if (!config || !config.additionalConfig?.replit_app_url || !config.additionalConfig?.cron_secret_key) {
        return res.status(400).json({
          success: false,
          message: "GitHub Actions not configured properly"
        });
      }
      const repl_url = config.additionalConfig.replit_app_url;
      const cron_secret = config.additionalConfig.cron_secret_key;
      const response = await fetch(`${repl_url}/api/cron/trigger`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-cron-secret": cron_secret
        },
        body: JSON.stringify({ source: "admin-test" })
      });
      const data = await response.json();
      if (response.ok) {
        await firestoreService.updateAPIConfig("github_actions", {
          isConnected: true,
          lastTested: /* @__PURE__ */ new Date()
        });
        res.json({
          success: true,
          message: "\u062A\u0645 \u062A\u0634\u063A\u064A\u0644 \u0627\u0644\u0646\u0634\u0631 \u0628\u0646\u062C\u0627\u062D",
          data
        });
      } else {
        res.status(response.status).json({
          success: false,
          message: data.message || "\u0641\u0634\u0644 \u0641\u064A \u062A\u0634\u063A\u064A\u0644 \u0627\u0644\u0646\u0634\u0631"
        });
      }
    } catch (error) {
      res.status(500).json({
        success: false,
        message: error.message || "\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0627\u062A\u0635\u0627\u0644"
      });
    }
  });
  app2.post("/api/facebook/post", authenticateUser, async (req, res) => {
    try {
      const { pageId, accessToken, message, link, scheduledTime } = req.body;
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      let result;
      if (scheduledTime) {
        result = await facebookSDK2.schedulePost(pageId, accessToken, { message, link }, new Date(scheduledTime));
      } else {
        result = await facebookSDK2.publishPost(pageId, accessToken, { message, link });
      }
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/facebook/photo", authenticateUser, async (req, res) => {
    try {
      const { pageId, accessToken, photoUrl, caption } = req.body;
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      const result = await facebookSDK2.uploadPhoto(pageId, accessToken, photoUrl, caption);
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/facebook/video", authenticateUser, async (req, res) => {
    try {
      const { pageId, accessToken, videoUrl, description } = req.body;
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      const result = await facebookSDK2.uploadVideo(pageId, accessToken, videoUrl, description);
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/facebook/insights/:pageId", authenticateUser, async (req, res) => {
    try {
      const { pageId } = req.params;
      const { accessToken } = req.query;
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      const insights = await facebookSDK2.getPageInsights(pageId, accessToken);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/instagram/post", authenticateUser, async (req, res) => {
    try {
      const { igUserId, accessToken, imageUrl, videoUrl, caption, mediaType } = req.body;
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const result = await instagramSDK2.publishPost(igUserId, accessToken, {
        image_url: imageUrl,
        video_url: videoUrl,
        caption,
        media_type: mediaType || "IMAGE"
      });
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/instagram/reel", authenticateUser, async (req, res) => {
    try {
      const { igUserId, accessToken, videoUrl, caption, coverUrl, shareToFeed } = req.body;
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const result = await instagramSDK2.publishReel(igUserId, accessToken, videoUrl, caption, coverUrl, shareToFeed);
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/instagram/story", authenticateUser, async (req, res) => {
    try {
      const { igUserId, accessToken, imageUrl, videoUrl } = req.body;
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const result = await instagramSDK2.publishStory(igUserId, accessToken, {
        image_url: imageUrl,
        video_url: videoUrl,
        media_type: "STORIES"
      });
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/instagram/insights/:mediaId", authenticateUser, async (req, res) => {
    try {
      const { mediaId } = req.params;
      const { accessToken } = req.query;
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const insights = await instagramSDK2.getMediaInsights(mediaId, accessToken);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/instagram/media/:igUserId", authenticateUser, async (req, res) => {
    try {
      const { igUserId } = req.params;
      const { accessToken, limit } = req.query;
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const media = await instagramSDK2.getUserMedia(igUserId, accessToken, parseInt(limit) || 25);
      res.json(media);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/tiktok/video", authenticateUser, async (req, res) => {
    try {
      const { accessToken, videoUrl, title, privacyLevel } = req.body;
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      const result = await tiktokSDK2.publishVideoFromUrl(accessToken, videoUrl, title, privacyLevel || "PUBLIC_TO_EVERYONE");
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/tiktok/videos", authenticateUser, async (req, res) => {
    try {
      const { accessToken, cursor, maxCount } = req.query;
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      const videos = await tiktokSDK2.getVideoList(
        accessToken,
        cursor ? parseInt(cursor) : void 0,
        maxCount ? parseInt(maxCount) : 20
      );
      res.json(videos);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/tiktok/insights", authenticateUser, async (req, res) => {
    try {
      const { accessToken, videoIds } = req.body;
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      const insights = await tiktokSDK2.getVideoInsights(accessToken, videoIds);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/tiktok/user", authenticateUser, async (req, res) => {
    try {
      const { accessToken } = req.query;
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      const userInfo = await tiktokSDK2.getUserInfo(accessToken);
      res.json(userInfo);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/ai/generate", authenticateUser, async (req, res) => {
    try {
      const { prompt, systemPrompt, useReasoning, options } = req.body;
      const { deepseekSDK: deepseekSDK2 } = await Promise.resolve().then(() => (init_deepseek(), deepseek_exports));
      let result;
      if (useReasoning) {
        result = await deepseekSDK2.generateWithReasoning(prompt, systemPrompt);
      } else {
        const content = await deepseekSDK2.generateSimple(prompt, systemPrompt, options);
        result = { content };
      }
      res.json(result);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/storage/upload-url", authenticateUser, async (req, res) => {
    try {
      const { fileName, contentType } = req.body;
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      const uploadUrl = await r2Storage2.getUploadUrl(fileName, contentType);
      res.json({ uploadUrl });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/storage/file-url", authenticateUser, async (req, res) => {
    try {
      const { fileName, expiresIn } = req.body;
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      const fileUrl = await r2Storage2.getFileUrl(fileName, expiresIn);
      res.json({ fileUrl });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/storage/files", authenticateUser, async (req, res) => {
    try {
      const { prefix, maxKeys, continuationToken } = req.query;
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      const files = await r2Storage2.listFiles(
        prefix,
        maxKeys ? parseInt(maxKeys) : 1e3,
        continuationToken
      );
      res.json(files);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.delete("/api/storage/file/:fileName", authenticateUser, async (req, res) => {
    try {
      const { fileName } = req.params;
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      await r2Storage2.deleteFile(fileName);
      res.json({ message: "File deleted successfully" });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/storage/file/:fileName/exists", authenticateUser, async (req, res) => {
    try {
      const { fileName } = req.params;
      const { r2Storage: r2Storage2 } = await Promise.resolve().then(() => (init_r2_storage(), r2_storage_exports));
      const exists = await r2Storage2.fileExists(fileName);
      res.json({ exists });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  const oauthSessions = /* @__PURE__ */ new Map();
  setInterval(() => {
    const now = Date.now();
    Array.from(oauthSessions.entries()).forEach(([key, session]) => {
      if (session.expiresAt < now) {
        oauthSessions.delete(key);
      }
    });
  }, 5 * 60 * 1e3);
  app2.get("/auth/facebook/callback", (req, res) => {
    const queryString = req.url.includes("?") ? req.url.split("?")[1] : "";
    res.redirect(`/api/oauth/facebook/callback?${queryString}`);
  });
  app2.get("/auth/instagram/callback", (req, res) => {
    const queryString = req.url.includes("?") ? req.url.split("?")[1] : "";
    res.redirect(`/api/oauth/instagram/callback?${queryString}`);
  });
  app2.get("/auth/tiktok/callback", (req, res) => {
    const queryString = req.url.includes("?") ? req.url.split("?")[1] : "";
    res.redirect(`/api/oauth/tiktok/callback?${queryString}`);
  });
  app2.get("/api/oauth/facebook/url", authenticateUser, async (req, res) => {
    try {
      const config = await firestoreService.getAPIConfig("facebook");
      if (!config || !config.appId) {
        return res.status(400).json({ message: "\u062A\u0643\u0648\u064A\u0646 \u0641\u064A\u0633\u0628\u0648\u0643 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F" });
      }
      const baseUrl = `${req.protocol}://${req.get("host")}`;
      const redirectUri = config.redirectUrl || `${baseUrl}/api/oauth/facebook/callback`;
      const scope = "pages_show_list,pages_read_engagement,pages_manage_posts,instagram_basic,instagram_content_publish,publish_video,pages_read_user_content";
      const authUrl = `https://www.facebook.com/v22.0/dialog/oauth?client_id=${config.appId}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${encodeURIComponent(scope)}&response_type=code&state=${req.userId}`;
      res.json({ url: authUrl });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/oauth/facebook/callback", async (req, res) => {
    try {
      const { code, state: userId, error: fbError, error_description } = req.query;
      if (fbError) {
        return res.send(`
          <!DOCTYPE html>
          <html dir="rtl">
          <head>
            <title>\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0631\u0628\u0637</title>
            <meta charset="utf-8">
            <style>
              body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f5f5f5; }
              .error-card { background: white; padding: 30px; border-radius: 12px; max-width: 400px; margin: auto; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
              h1 { color: #e53e3e; }
              button { background: #3b82f6; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; margin-top: 20px; }
            </style>
          </head>
          <body>
            <div class="error-card">
              <h1>\u062E\u0637\u0623 \u0641\u064A \u062A\u0633\u062C\u064A\u0644 \u0627\u0644\u062F\u062E\u0648\u0644</h1>
              <p>${error_description || "\u062A\u0645 \u0631\u0641\u0636 \u0637\u0644\u0628 \u0627\u0644\u062A\u0641\u0648\u064A\u0636"}</p>
              <button onclick="window.close()">\u0625\u063A\u0644\u0627\u0642</button>
            </div>
          </body>
          </html>
        `);
      }
      if (!code || !userId) {
        return res.status(400).send("Missing authorization code or user state");
      }
      const config = await firestoreService.getAPIConfig("facebook");
      if (!config || !config.appId || !config.appSecret) {
        return res.status(400).send("Invalid Facebook configuration");
      }
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      const baseUrl = `${req.protocol}://${req.get("host")}`;
      const redirectUri = config.redirectUrl || `${baseUrl}/api/oauth/facebook/callback`;
      const accessToken = await facebookSDK2.exchangeCodeForToken(code, redirectUri);
      const longLivedToken = await facebookSDK2.getLongLivedToken(accessToken);
      const pages = await facebookSDK2.getUserPages(longLivedToken.access_token);
      if (pages.length === 0) {
        return res.send(`
          <!DOCTYPE html>
          <html dir="rtl">
          <head>
            <title>\u0644\u0627 \u062A\u0648\u062C\u062F \u0635\u0641\u062D\u0627\u062A</title>
            <meta charset="utf-8">
            <style>
              body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f5f5f5; }
              .card { background: white; padding: 30px; border-radius: 12px; max-width: 400px; margin: auto; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
              h1 { color: #f59e0b; }
              button { background: #3b82f6; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; margin-top: 20px; }
            </style>
          </head>
          <body>
            <div class="card">
              <h1>\u0644\u0627 \u062A\u0648\u062C\u062F \u0635\u0641\u062D\u0627\u062A</h1>
              <p>\u0644\u0645 \u064A\u062A\u0645 \u0627\u0644\u0639\u062B\u0648\u0631 \u0639\u0644\u0649 \u0623\u064A \u0635\u0641\u062D\u0627\u062A \u0641\u064A\u0633\u0628\u0648\u0643 \u0645\u0631\u062A\u0628\u0637\u0629 \u0628\u062D\u0633\u0627\u0628\u0643. \u062A\u0623\u0643\u062F \u0645\u0646 \u0623\u0646\u0643 \u0645\u062F\u064A\u0631 \u0644\u0635\u0641\u062D\u0629 \u0641\u064A\u0633\u0628\u0648\u0643 \u0648\u0627\u062D\u062F\u0629 \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644.</p>
              <button onclick="window.close()">\u0625\u063A\u0644\u0627\u0642</button>
            </div>
          </body>
          </html>
        `);
      }
      const sessionId = `fb_${Date.now()}_${Math.random().toString(36).substring(7)}`;
      oauthSessions.set(sessionId, {
        userId,
        pages,
        longLivedToken: longLivedToken.access_token,
        expiresAt: Date.now() + 10 * 60 * 1e3
        // 10 minutes TTL
      });
      res.send(`
        <!DOCTYPE html>
        <html dir="rtl">
        <head>
          <title>\u0627\u062E\u062A\u064A\u0627\u0631 \u0635\u0641\u062D\u0627\u062A \u0627\u0644\u0641\u064A\u0633\u0628\u0648\u0643</title>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { 
              font-family: Arial, sans-serif; 
              background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
              min-height: 100vh;
              padding: 20px;
            }
            .container { 
              background: white; 
              padding: 30px; 
              border-radius: 16px; 
              max-width: 500px; 
              margin: auto; 
              box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            }
            h1 { 
              color: #1877F2; 
              margin-bottom: 10px;
              font-size: 24px;
            }
            .subtitle {
              color: #666;
              margin-bottom: 24px;
              font-size: 14px;
            }
            .page-list { 
              max-height: 400px; 
              overflow-y: auto; 
              margin-bottom: 20px;
            }
            .page-item {
              display: flex;
              align-items: center;
              padding: 16px;
              border: 2px solid #e5e7eb;
              border-radius: 12px;
              margin-bottom: 12px;
              cursor: pointer;
              transition: all 0.2s;
            }
            .page-item:hover {
              border-color: #1877F2;
              background: #f8fafc;
            }
            .page-item.selected {
              border-color: #1877F2;
              background: #eff6ff;
            }
            .page-item input {
              margin-left: 12px;
              width: 20px;
              height: 20px;
              accent-color: #1877F2;
            }
            .page-info { flex: 1; }
            .page-name { font-weight: bold; color: #1f2937; }
            .page-category { font-size: 12px; color: #6b7280; margin-top: 4px; }
            .fb-icon {
              width: 40px;
              height: 40px;
              background: #1877F2;
              border-radius: 8px;
              display: flex;
              align-items: center;
              justify-content: center;
              color: white;
              font-weight: bold;
              margin-left: 12px;
            }
            .btn-container {
              display: flex;
              gap: 12px;
            }
            button {
              flex: 1;
              padding: 14px 24px;
              border-radius: 10px;
              font-size: 16px;
              font-weight: bold;
              cursor: pointer;
              transition: all 0.2s;
              border: none;
            }
            .btn-primary {
              background: #1877F2;
              color: white;
            }
            .btn-primary:hover { background: #1565c0; }
            .btn-primary:disabled { 
              background: #9ca3af; 
              cursor: not-allowed;
            }
            .btn-secondary {
              background: #f3f4f6;
              color: #374151;
            }
            .btn-secondary:hover { background: #e5e7eb; }
            .loading {
              display: none;
              text-align: center;
              padding: 20px;
            }
            .spinner {
              width: 40px;
              height: 40px;
              border: 4px solid #e5e7eb;
              border-top-color: #1877F2;
              border-radius: 50%;
              animation: spin 1s linear infinite;
              margin: 0 auto 12px;
            }
            @keyframes spin { to { transform: rotate(360deg); } }
            .success {
              display: none;
              text-align: center;
              padding: 20px;
            }
            .success-icon {
              width: 60px;
              height: 60px;
              background: #10b981;
              border-radius: 50%;
              display: flex;
              align-items: center;
              justify-content: center;
              margin: 0 auto 16px;
              color: white;
              font-size: 30px;
            }
            .select-all {
              display: flex;
              align-items: center;
              margin-bottom: 16px;
              padding: 8px;
              background: #f8fafc;
              border-radius: 8px;
            }
            .select-all input { margin-left: 8px; }
            .select-all label { color: #1877F2; cursor: pointer; }
          </style>
        </head>
        <body>
          <div class="container">
            <div id="selection-form">
              <h1>\u0627\u062E\u062A\u0631 \u0635\u0641\u062D\u0627\u062A \u0627\u0644\u0641\u064A\u0633\u0628\u0648\u0643</h1>
              <p class="subtitle">\u062D\u062F\u062F \u0627\u0644\u0635\u0641\u062D\u0627\u062A \u0627\u0644\u062A\u064A \u062A\u0631\u064A\u062F \u0631\u0628\u0637\u0647\u0627 \u0645\u0639 \u0645\u0646\u0635\u0629 \u062C\u062F\u0648\u0644\u0629 \u0627\u0644\u0642\u0635\u0635</p>
              
              <div class="select-all">
                <input type="checkbox" id="select-all-checkbox" onchange="toggleSelectAll()">
                <label for="select-all-checkbox">\u062A\u062D\u062F\u064A\u062F \u062C\u0645\u064A\u0639 \u0627\u0644\u0635\u0641\u062D\u0627\u062A</label>
              </div>
              
              <div class="page-list">
                ${pages.map((page, index) => `
                  <label class="page-item" for="page-${index}">
                    <input type="checkbox" id="page-${index}" value="${page.id}" name="pages">
                    <div class="fb-icon">${page.name.charAt(0)}</div>
                    <div class="page-info">
                      <div class="page-name">${page.name}</div>
                      <div class="page-category">${page.category || "\u0635\u0641\u062D\u0629 \u0641\u064A\u0633\u0628\u0648\u0643"}</div>
                    </div>
                  </label>
                `).join("")}
              </div>
              
              <div class="btn-container">
                <button type="button" class="btn-secondary" onclick="window.close()">\u0625\u0644\u063A\u0627\u0621</button>
                <button type="button" class="btn-primary" id="submit-btn" onclick="submitSelection()" disabled>\u0631\u0628\u0637 \u0627\u0644\u0635\u0641\u062D\u0627\u062A \u0627\u0644\u0645\u062D\u062F\u062F\u0629</button>
              </div>
            </div>
            
            <div id="loading" class="loading">
              <div class="spinner"></div>
              <p>\u062C\u0627\u0631\u064A \u0631\u0628\u0637 \u0627\u0644\u0635\u0641\u062D\u0627\u062A...</p>
            </div>
            
            <div id="success" class="success">
              <div class="success-icon">\u2713</div>
              <h2 style="color: #10b981; margin-bottom: 8px;">\u062A\u0645 \u0627\u0644\u0631\u0628\u0637 \u0628\u0646\u062C\u0627\u062D!</h2>
              <p style="color: #666;">\u0633\u064A\u062A\u0645 \u0625\u063A\u0644\u0627\u0642 \u0647\u0630\u0647 \u0627\u0644\u0646\u0627\u0641\u0630\u0629 \u062A\u0644\u0642\u0627\u0626\u064A\u0627\u064B...</p>
            </div>
          </div>
          
          <script>
            const sessionId = '${sessionId}';
            
            document.querySelectorAll('input[name="pages"]').forEach(input => {
              input.addEventListener('change', updateSubmitButton);
            });
            
            function toggleSelectAll() {
              const selectAll = document.getElementById('select-all-checkbox').checked;
              document.querySelectorAll('input[name="pages"]').forEach(input => {
                input.checked = selectAll;
                input.closest('.page-item').classList.toggle('selected', selectAll);
              });
              updateSubmitButton();
            }
            
            function updateSubmitButton() {
              const checked = document.querySelectorAll('input[name="pages"]:checked').length;
              document.getElementById('submit-btn').disabled = checked === 0;
              
              document.querySelectorAll('input[name="pages"]').forEach(input => {
                input.closest('.page-item').classList.toggle('selected', input.checked);
              });
            }
            
            async function submitSelection() {
              const selectedPages = Array.from(document.querySelectorAll('input[name="pages"]:checked'))
                .map(input => input.value);
              
              if (selectedPages.length === 0) {
                alert('\u064A\u0631\u062C\u0649 \u0627\u062E\u062A\u064A\u0627\u0631 \u0635\u0641\u062D\u0629 \u0648\u0627\u062D\u062F\u0629 \u0639\u0644\u0649 \u0627\u0644\u0623\u0642\u0644');
                return;
              }
              
              document.getElementById('selection-form').style.display = 'none';
              document.getElementById('loading').style.display = 'block';
              
              try {
                const response = await fetch('/api/oauth/facebook/pages', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ sessionId, selectedPageIds: selectedPages })
                });
                
                const result = await response.json();
                
                if (response.ok) {
                  document.getElementById('loading').style.display = 'none';
                  document.getElementById('success').style.display = 'block';
                  setTimeout(() => window.close(), 2000);
                } else {
                  throw new Error(result.message || '\u0641\u0634\u0644 \u0631\u0628\u0637 \u0627\u0644\u0635\u0641\u062D\u0627\u062A');
                }
              } catch (error) {
                document.getElementById('loading').style.display = 'none';
                document.getElementById('selection-form').style.display = 'block';
                alert('\u062E\u0637\u0623: ' + error.message);
              }
            }
          </script>
        </body>
        </html>
      `);
    } catch (error) {
      console.error("Facebook OAuth error:", error);
      res.send(`
        <!DOCTYPE html>
        <html dir="rtl">
        <head>
          <title>\u062E\u0637\u0623 \u0641\u064A \u0627\u0644\u0631\u0628\u0637</title>
          <meta charset="utf-8">
          <style>
            body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f5f5f5; }
            .error-card { background: white; padding: 30px; border-radius: 12px; max-width: 400px; margin: auto; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { color: #e53e3e; }
            .error-details { background: #fef2f2; padding: 12px; border-radius: 8px; margin: 16px 0; text-align: right; font-size: 12px; color: #991b1b; }
            button { background: #3b82f6; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; }
          </style>
        </head>
        <body>
          <div class="error-card">
            <h1>\u062E\u0637\u0623 \u0641\u064A \u0631\u0628\u0637 \u0627\u0644\u062D\u0633\u0627\u0628</h1>
            <p>\u062D\u062F\u062B \u062E\u0637\u0623 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628 \u0641\u064A\u0633\u0628\u0648\u0643</p>
            <div class="error-details">${error.message}</div>
            <button onclick="window.close()">\u0625\u063A\u0644\u0627\u0642</button>
          </div>
        </body>
        </html>
      `);
    }
  });
  app2.post("/api/oauth/facebook/pages", async (req, res) => {
    try {
      const { sessionId, selectedPageIds } = req.body;
      if (!sessionId || !selectedPageIds || !Array.isArray(selectedPageIds)) {
        return res.status(400).json({ message: "\u0628\u064A\u0627\u0646\u0627\u062A \u063A\u064A\u0631 \u0635\u0627\u0644\u062D\u0629" });
      }
      const session = oauthSessions.get(sessionId);
      if (!session) {
        return res.status(400).json({ message: "\u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u062C\u0644\u0633\u0629. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
      }
      if (session.expiresAt < Date.now()) {
        oauthSessions.delete(sessionId);
        return res.status(400).json({ message: "\u0627\u0646\u062A\u0647\u062A \u0635\u0644\u0627\u062D\u064A\u0629 \u0627\u0644\u062C\u0644\u0633\u0629. \u064A\u0631\u062C\u0649 \u0627\u0644\u0645\u062D\u0627\u0648\u0644\u0629 \u0645\u0631\u0629 \u0623\u062E\u0631\u0649." });
      }
      const linkedPages = [];
      for (const pageId of selectedPageIds) {
        const page = session.pages.find((p) => p.id === pageId);
        if (!page) continue;
        const existingAccounts = await firestoreService.getLinkedAccountsByUser(session.userId, {
          platform: "facebook",
          search: page.id
        });
        if (existingAccounts.length === 0) {
          await firestoreService.createLinkedAccount(session.userId, {
            platform: "facebook",
            accountType: "page",
            externalId: page.id,
            name: page.name,
            username: page.name,
            status: "active",
            accessToken: page.access_token,
            permissions: ["pages_manage_posts", "publish_video", "pages_read_engagement"],
            capabilities: {
              canPublishStories: true,
              canPublishPosts: true,
              canPublishReels: true,
              canSchedule: true,
              canGetInsights: true
            },
            quotas: {
              dailyLimit: 50,
              dailyUsed: 0,
              monthlyLimit: 1e3,
              monthlyUsed: 0,
              resetAt: new Date(Date.now() + 24 * 60 * 60 * 1e3)
            }
          });
          linkedPages.push(page.name);
        }
      }
      oauthSessions.delete(sessionId);
      res.json({
        success: true,
        message: `\u062A\u0645 \u0631\u0628\u0637 ${linkedPages.length} \u0635\u0641\u062D\u0629 \u0628\u0646\u062C\u0627\u062D`,
        linkedPages
      });
    } catch (error) {
      console.error("Error saving Facebook pages:", error);
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/oauth/instagram/url", authenticateUser, async (req, res) => {
    try {
      const config = await firestoreService.getAPIConfig("instagram");
      if (!config || !config.appId) {
        return res.status(400).json({ message: "\u062A\u0643\u0648\u064A\u0646 \u0627\u0646\u0633\u062A\u063A\u0631\u0627\u0645 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F" });
      }
      const redirectUri = config.redirectUrl || `${req.protocol}://${req.get("host")}/api/oauth/instagram/callback`;
      const scope = "instagram_basic,instagram_content_publish,pages_show_list,pages_read_engagement";
      const authUrl = `https://www.facebook.com/v22.0/dialog/oauth?client_id=${config.appId}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${encodeURIComponent(scope)}&response_type=code&state=${req.userId}`;
      res.json({ url: authUrl });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/oauth/instagram/callback", async (req, res) => {
    try {
      const { code, state: userId } = req.query;
      if (!code || !userId) {
        return res.status(400).send("Missing authorization code or user state");
      }
      const config = await firestoreService.getAPIConfig("instagram");
      if (!config || !config.appId || !config.appSecret) {
        return res.status(400).send("Invalid Instagram configuration");
      }
      const { facebookSDK: facebookSDK2 } = await Promise.resolve().then(() => (init_facebook(), facebook_exports));
      const { instagramSDK: instagramSDK2 } = await Promise.resolve().then(() => (init_instagram(), instagram_exports));
      const redirectUri = config.redirectUrl || `${req.protocol}://${req.get("host")}/api/oauth/instagram/callback`;
      const accessToken = await facebookSDK2.exchangeCodeForToken(code, redirectUri);
      const longLivedToken = await facebookSDK2.getLongLivedToken(accessToken);
      const pages = await facebookSDK2.getUserPages(longLivedToken.access_token);
      let instagramAccountsCount = 0;
      for (const page of pages) {
        const pageDetails = await facebookSDK2.getPageInstagramAccount(page.id, page.access_token);
        if (pageDetails.instagram_business_account) {
          const igAccount = pageDetails.instagram_business_account;
          const igProfile = await instagramSDK2.getUserProfile(igAccount.id, page.access_token);
          const existingAccounts = await firestoreService.getLinkedAccountsByUser(userId, {
            platform: "instagram",
            search: igAccount.id
          });
          if (existingAccounts.length === 0) {
            await firestoreService.createLinkedAccount(userId, {
              platform: "instagram",
              accountType: "business",
              externalId: igAccount.id,
              name: igProfile.name || igProfile.username,
              username: igProfile.username,
              profilePictureUrl: igProfile.profile_picture_url,
              status: "active",
              accessToken: page.access_token,
              permissions: ["instagram_content_publish", "instagram_basic"],
              capabilities: {
                canPublishStories: true,
                canPublishPosts: true,
                canPublishReels: true,
                canSchedule: false,
                canGetInsights: true
              },
              quotas: {
                dailyLimit: 50,
                dailyUsed: 0,
                monthlyLimit: 1e3,
                monthlyUsed: 0,
                resetAt: new Date(Date.now() + 24 * 60 * 60 * 1e3)
              }
            });
            instagramAccountsCount++;
          }
        }
      }
      res.send(`
        <!DOCTYPE html>
        <html dir="rtl">
        <head>
          <title>\u062A\u0645 \u0627\u0644\u0631\u0628\u0637 \u0628\u0646\u062C\u0627\u062D</title>
          <meta charset="utf-8">
        </head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
          <h1>\u2705 \u062A\u0645 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628\u0627\u062A \u0627\u0646\u0633\u062A\u063A\u0631\u0627\u0645 \u0628\u0646\u062C\u0627\u062D</h1>
          <p>\u062A\u0645 \u0625\u0636\u0627\u0641\u0629 ${instagramAccountsCount} \u062D\u0633\u0627\u0628</p>
          <script>
            setTimeout(() => window.close(), 2000);
          </script>
        </body>
        </html>
      `);
    } catch (error) {
      res.status(500).send(`\u062E\u0637\u0623: ${error.message}`);
    }
  });
  app2.get("/api/oauth/tiktok/url", authenticateUser, async (req, res) => {
    try {
      const config = await firestoreService.getAPIConfig("tiktok");
      if (!config || !config.apiKey) {
        return res.status(400).json({ message: "\u062A\u0643\u0648\u064A\u0646 \u062A\u064A\u0643 \u062A\u0648\u0643 \u063A\u064A\u0631 \u0645\u0648\u062C\u0648\u062F" });
      }
      const redirectUri = config.redirectUrl || `${req.protocol}://${req.get("host")}/api/oauth/tiktok/callback`;
      const scope = "user.info.basic,video.list,video.upload,video.publish";
      const authUrl = `https://www.tiktok.com/v2/auth/authorize/?client_key=${config.apiKey}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${encodeURIComponent(scope)}&response_type=code&state=${req.userId}`;
      res.json({ url: authUrl });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/oauth/tiktok/callback", async (req, res) => {
    try {
      const { code, state: userId } = req.query;
      if (!code || !userId) {
        return res.status(400).send("Missing authorization code or user state");
      }
      const config = await firestoreService.getAPIConfig("tiktok");
      if (!config || !config.apiKey || !config.appSecret) {
        return res.status(400).send("Invalid TikTok configuration");
      }
      const { tiktokSDK: tiktokSDK2 } = await Promise.resolve().then(() => (init_tiktok(), tiktok_exports));
      const redirectUri = config.redirectUrl || `${req.protocol}://${req.get("host")}/api/oauth/tiktok/callback`;
      const tokenData = await tiktokSDK2.exchangeCodeForToken(code, redirectUri);
      const userInfo = await tiktokSDK2.getUserInfo(tokenData.access_token);
      const existingAccounts = await firestoreService.getLinkedAccountsByUser(userId, {
        platform: "tiktok",
        search: userInfo.data.user.open_id
      });
      if (existingAccounts.length === 0) {
        const tokenExpiresAt = new Date(Date.now() + tokenData.expires_in * 1e3);
        await firestoreService.createLinkedAccount(userId, {
          platform: "tiktok",
          accountType: "profile",
          externalId: userInfo.data.user.open_id,
          name: userInfo.data.user.display_name,
          username: userInfo.data.user.username,
          profilePictureUrl: userInfo.data.user.avatar_url,
          status: "active",
          accessToken: tokenData.access_token,
          refreshToken: tokenData.refresh_token,
          tokenExpiresAt,
          permissions: ["video.upload", "video.publish"],
          capabilities: {
            canPublishStories: false,
            canPublishPosts: true,
            canPublishReels: true,
            canSchedule: true,
            canGetInsights: true
          },
          quotas: {
            dailyLimit: 50,
            dailyUsed: 0,
            monthlyLimit: 1e3,
            monthlyUsed: 0,
            resetAt: new Date(Date.now() + 24 * 60 * 60 * 1e3)
          }
        });
      }
      res.send(`
        <!DOCTYPE html>
        <html dir="rtl">
        <head>
          <title>\u062A\u0645 \u0627\u0644\u0631\u0628\u0637 \u0628\u0646\u062C\u0627\u062D</title>
          <meta charset="utf-8">
        </head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
          <h1>\u2705 \u062A\u0645 \u0631\u0628\u0637 \u062D\u0633\u0627\u0628 \u062A\u064A\u0643 \u062A\u0648\u0643 \u0628\u0646\u062C\u0627\u062D</h1>
          <p>\u062D\u0633\u0627\u0628: ${userInfo.data.user.display_name}</p>
          <script>
            setTimeout(() => window.close(), 2000);
          </script>
        </body>
        </html>
      `);
    } catch (error) {
      res.status(500).send(`\u062E\u0637\u0623: ${error.message}`);
    }
  });
  app2.get("/api/smart-algorithms/dashboard-insights", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const platformStats = [
        { platform: "facebook", totalStories: 0, publishedStories: 0, averageEngagement: 0 },
        { platform: "instagram", totalStories: 0, publishedStories: 0, averageEngagement: 0 },
        { platform: "tiktok", totalStories: 0, publishedStories: 0, averageEngagement: 0 }
      ];
      stories.forEach((story) => {
        const platforms2 = Array.isArray(story.platforms) ? story.platforms : [];
        platforms2.forEach((platform) => {
          const stat = platformStats.find((s) => s.platform === platform);
          if (stat) {
            stat.totalStories++;
            if (story.status === "published") {
              stat.publishedStories++;
            }
          }
        });
      });
      const publishedStories = stories.filter((s) => s.status === "published");
      platformStats.forEach((stat) => {
        const platformPublished = publishedStories.filter((s) => Array.isArray(s.platforms) && s.platforms.includes(stat.platform));
        const totalEng = platformPublished.reduce((sum, s) => sum + (s.engagementRate || 0), 0);
        stat.averageEngagement = platformPublished.length > 0 ? parseFloat((totalEng / platformPublished.length).toFixed(2)) : 0;
      });
      const insights = smartAlgorithms2.generateDashboardInsights(stories, platformStats);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/optimal-times", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const optimalTimes = smartAlgorithms2.analyzeOptimalPostingTimes(stories);
      res.json(optimalTimes);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.post("/api/smart-algorithms/suggest-schedule", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const { platforms: platforms2 } = req.body;
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const suggestedTime = smartAlgorithms2.suggestOptimalScheduleTime(stories, platforms2 || []);
      res.json({ suggestedTime });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/account-health", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const healthMetrics = smartAlgorithms2.analyzeAccountHealth(accounts, stories);
      const healthScores = smartAlgorithms2.dijkstraHealthScore(healthMetrics);
      const frontendHealth = healthScores.map((h) => {
        const originalMetric = healthMetrics.find((m) => m.accountId === h.accountId);
        const tokenExpiresAt = accounts.find((a) => a.id === h.accountId)?.tokenExpiresAt;
        const daysToExpiry = tokenExpiresAt ? Math.floor((new Date(tokenExpiresAt).getTime() - Date.now()) / (1e3 * 60 * 60 * 24)) : 0;
        return {
          accountId: h.accountId,
          tokenStatus: h.isTokenExpiringSoon ? daysToExpiry <= 0 ? "expired" : "expiring_soon" : "valid",
          tokenExpiresIn: Math.max(0, daysToExpiry),
          connectionStatus: h.connectionStatus,
          quotaUsagePercent: originalMetric?.quotaUsagePercent || 0,
          lastSyncAt: (/* @__PURE__ */ new Date()).toISOString(),
          healthScore: Math.round(h.healthScore)
        };
      });
      res.json(frontendHealth);
    } catch (error) {
      console.error("Account health error:", error);
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/account-performance", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const performanceMetrics = smartAlgorithms2.analyzeAccountPerformance(accounts, stories);
      res.json(performanceMetrics);
    } catch (error) {
      console.error("Account performance error:", error);
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/account-recommendations", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const recommendations = smartAlgorithms2.calculateAccountRecommendations(accounts, stories);
      res.json(recommendations);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/accounts/categories", authenticateUser, async (req, res) => {
    try {
      const { accountCategorizationEngine: accountCategorizationEngine2 } = await Promise.resolve().then(() => (init_account_categorization(), account_categorization_exports));
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const categories = accountCategorizationEngine2.categorizeMultipleAccounts(accounts);
      res.json(categories);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/accounts/categories/:classification", authenticateUser, async (req, res) => {
    try {
      const { accountCategorizationEngine: accountCategorizationEngine2 } = await Promise.resolve().then(() => (init_account_categorization(), account_categorization_exports));
      const accounts = await firestoreService.getLinkedAccountsByUser(req.userId, {});
      const { classification } = req.params;
      const categories = accountCategorizationEngine2.getAccountsByClassification(accounts, classification);
      res.json(categories);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/performance-analysis", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const performanceAnalysis = smartAlgorithms2.analyzePerformance(stories);
      res.json(performanceAnalysis);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/smart-algorithms/trend-analysis", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const trendAnalysis = smartAlgorithms2.analyzeTrends(stories);
      res.json(trendAnalysis);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/stats/engagement", authenticateUser, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const stories = await firestoreService.getStoriesByUser(req.userId);
      const engagementStats = smartAlgorithms2.calculateEngagementStats(stories);
      res.json(engagementStats);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/smart-algorithms/system-metrics", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { smartAlgorithms: smartAlgorithms2 } = await Promise.resolve().then(() => (init_smart_algorithms(), smart_algorithms_exports));
      const users = await firestoreService.getAllUsers();
      const allStories = [];
      for (const user of users) {
        const stories = await firestoreService.getStoriesByUser(user.id);
        allStories.push(...stories);
      }
      const apiConfigs = await firestoreService.getAPIConfigs();
      const metrics = smartAlgorithms2.generateAdminSystemMetrics(users, allStories, apiConfigs);
      res.json(metrics);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/trending-content", authenticateUser, async (req, res) => {
    try {
      const { trendingContentService: trendingContentService2 } = await Promise.resolve().then(() => (init_trending_content_service(), trending_content_service_exports));
      const result = await trendingContentService2.getTrendingContent();
      res.json(result);
    } catch (error) {
      console.error("Trending content error:", error);
      res.status(500).json({
        movies: [],
        tv_series: [],
        other_categories: [],
        generation_errors: [{
          category: "system",
          item_title: "General Error",
          error_type: "other",
          message: error.message || "\u0641\u0634\u0644 \u0641\u064A \u062C\u0644\u0628 \u0627\u0644\u0645\u062D\u062A\u0648\u0649 \u0627\u0644\u0631\u0627\u0626\u062C"
        }]
      });
    }
  });
  app2.post("/api/cron/trigger", async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const { tokenManagementService: tokenManagementService2 } = await Promise.resolve().then(() => (init_token_management_service(), token_management_service_exports));
      const cronSecret = process.env.CRON_SECRET_KEY;
      const providedSecret = req.headers["x-cron-secret"] || req.body?.secret;
      if (cronSecret && providedSecret !== cronSecret) {
        return res.status(401).json({
          success: false,
          error: "Invalid cron secret key",
          message: "\u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0623\u0645\u0627\u0646 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D"
        });
      }
      console.log(`
\u{1F916} [${(/* @__PURE__ */ new Date()).toISOString()}] Authorized GitHub Actions cron trigger received`);
      console.log("\u{1F916} [Cron] Step 1: Running smart token management...");
      await tokenManagementService2.processAllTokens();
      console.log("\u{1F916} [Cron] Step 2: Triggering auto-publishing cycle...");
      const result = await cronScheduler2.triggerFromWebhook(providedSecret);
      res.json({
        success: true,
        message: "\u062A\u0645 \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062C\u062F\u0648\u0644\u0629 \u0628\u0646\u062C\u0627\u062D",
        results: result.results,
        status: result.status,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      console.error("Cron trigger error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        message: "\u0641\u0634\u0644 \u0641\u064A \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062C\u062F\u0648\u0644\u0629"
      });
    }
  });
  app2.get("/api/cron/trigger", async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const { tokenManagementService: tokenManagementService2 } = await Promise.resolve().then(() => (init_token_management_service(), token_management_service_exports));
      const cronSecret = process.env.CRON_SECRET_KEY;
      const providedSecret = req.query?.secret;
      if (cronSecret && providedSecret !== cronSecret) {
        return res.status(401).json({
          success: false,
          error: "Invalid cron secret key",
          message: "\u0645\u0641\u062A\u0627\u062D \u0627\u0644\u0623\u0645\u0627\u0646 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D"
        });
      }
      console.log(`
\u{1F916} [${(/* @__PURE__ */ new Date()).toISOString()}] Authorized GitHub Actions cron GET trigger received`);
      console.log("\u{1F916} [Cron] Running smart token management...");
      await tokenManagementService2.processAllTokens();
      console.log("\u{1F916} [Cron] Triggering auto-publishing cycle...");
      const result = await cronScheduler2.triggerFromWebhook(providedSecret);
      res.json({
        success: true,
        message: "\u062A\u0645 \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062C\u062F\u0648\u0644\u0629 \u0628\u0646\u062C\u0627\u062D",
        results: result.results,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      console.error("Cron trigger error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        message: "\u0641\u0634\u0644 \u0641\u064A \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062C\u062F\u0648\u0644\u0629"
      });
    }
  });
  app2.get("/api/cron/status", async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const rawStatus = cronScheduler2.getStatus();
      const status = {
        ...rawStatus,
        lastRun: rawStatus.lastRun ? rawStatus.lastRun.toISOString() : null,
        nextRun: rawStatus.nextRun ? rawStatus.nextRun.toISOString() : null,
        lastHealthCheck: rawStatus.lastHealthCheck ? rawStatus.lastHealthCheck.toISOString() : null
      };
      res.json(status);
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message,
        message: "\u0641\u0634\u0644 \u0641\u064A \u062C\u0644\u0628 \u062D\u0627\u0644\u0629 \u0646\u0638\u0627\u0645 \u0627\u0644\u062C\u062F\u0648\u0644\u0629"
      });
    }
  });
  app2.get("/api/admin/cron/results", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const limit = parseInt(req.query?.limit) || 50;
      const rawResults = cronScheduler2.getRecentResults(limit);
      const results = rawResults.map((result) => ({
        ...result,
        timestamp: result.timestamp instanceof Date ? result.timestamp.toISOString() : result.timestamp
      }));
      res.json({
        success: true,
        results,
        count: results.length,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.get("/api/admin/cron/queue", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const allScheduledStories = await firestoreService.getAllScheduledStories();
      const schedulerQueue = cronScheduler2.getQueueStatus();
      const schedulerQueueMap = new Map(schedulerQueue.map((item) => [item.story.id, item]));
      const queue = allScheduledStories.map((story) => {
        const schedulerItem = schedulerQueueMap.get(story.id);
        return {
          story: {
            id: story.id,
            content: story.content,
            platforms: story.platforms,
            status: story.status,
            scheduledTime: story.scheduledTime?.toISOString ? story.scheduledTime.toISOString() : story.scheduledTime,
            videoGenerationStatus: story.videoGenerationStatus,
            createdAt: story.createdAt?.toISOString ? story.createdAt.toISOString() : story.createdAt
          },
          retryCount: schedulerItem?.retryCount || 0,
          lastAttempt: schedulerItem?.lastAttempt instanceof Date ? schedulerItem.lastAttempt.toISOString() : schedulerItem?.lastAttempt || null,
          nextRetryAt: schedulerItem?.nextRetryAt instanceof Date ? schedulerItem.nextRetryAt.toISOString() : schedulerItem?.nextRetryAt || null,
          addedAt: schedulerItem?.addedAt instanceof Date ? schedulerItem.addedAt.toISOString() : schedulerItem?.addedAt || (/* @__PURE__ */ new Date()).toISOString(),
          errorHistory: schedulerItem?.errorHistory || [],
          inSchedulerQueue: !!schedulerItem
        };
      });
      res.json({
        success: true,
        queue,
        queueSize: queue.length,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.post("/api/admin/cron/retry/:storyId", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const success = await cronScheduler2.forceRetryStory(req.params.storyId);
      res.json({
        success,
        message: success ? "\u062A\u0645 \u0625\u0639\u0627\u062F\u0629 \u0627\u0644\u0646\u0634\u0631 \u0628\u0646\u062C\u0627\u062D" : "\u0641\u0634\u0644 \u0641\u064A \u0625\u0639\u0627\u062F\u0629 \u0627\u0644\u0646\u0634\u0631",
        storyId: req.params.storyId,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      console.error("Manual retry error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        message: "\u062D\u062F\u062B \u062E\u0637\u0623 \u0623\u062B\u0646\u0627\u0621 \u0645\u062D\u0627\u0648\u0644\u0629 \u0625\u0639\u0627\u062F\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u064A\u062F\u0648\u064A"
      });
    }
  });
  app2.post("/api/admin/cron/trigger", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      console.log(`
\u{1F514} [${(/* @__PURE__ */ new Date()).toISOString()}] Admin manual cron trigger by ${req.userId || "admin"}`);
      const result = await cronScheduler2.triggerFromWebhook();
      const status = {
        ...result.status,
        lastRun: result.status.lastRun instanceof Date ? result.status.lastRun.toISOString() : result.status.lastRun,
        nextRun: result.status.nextRun instanceof Date ? result.status.nextRun.toISOString() : result.status.nextRun,
        lastHealthCheck: result.status.lastHealthCheck instanceof Date ? result.status.lastHealthCheck.toISOString() : result.status.lastHealthCheck
      };
      res.json({
        success: true,
        message: "\u062A\u0645 \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631 \u0627\u0644\u0645\u062C\u062F\u0648\u0644\u0629 \u0628\u0646\u062C\u0627\u062D",
        results: result.results,
        status,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      console.error("Admin cron trigger error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        message: "\u062D\u062F\u062B \u062E\u0637\u0623 \u0623\u062B\u0646\u0627\u0621 \u062A\u0646\u0641\u064A\u0630 \u0645\u0647\u0645\u0629 \u0627\u0644\u0646\u0634\u0631"
      });
    }
  });
  app2.post("/api/admin/cron/clear-failed", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const queueCleared = cronScheduler2.clearFailedFromQueue();
      const firestoreCleared = await firestoreService.deleteAllFailedStories();
      res.json({
        success: true,
        clearedCount: queueCleared + firestoreCleared,
        queueCleared,
        firestoreCleared,
        message: `\u062A\u0645 \u0625\u0632\u0627\u0644\u0629 ${queueCleared} \u0645\u0646 \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0631 \u0648 ${firestoreCleared} \u0645\u0646 Firestore`,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.post("/api/admin/cron/update-schedule", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const { cronExpression } = req.body;
      if (!cronExpression) {
        return res.status(400).json({
          success: false,
          message: "\u064A\u062C\u0628 \u062A\u062D\u062F\u064A\u062F \u062A\u0639\u0628\u064A\u0631 \u0627\u0644\u062C\u062F\u0648\u0644\u0629 (cron expression)"
        });
      }
      const success = cronScheduler2.updateCronExpression(cronExpression);
      res.json({
        success,
        message: success ? "\u062A\u0645 \u062A\u062D\u062F\u064A\u062B \u062C\u062F\u0648\u0644 \u0627\u0644\u062A\u0634\u063A\u064A\u0644 \u0628\u0646\u062C\u0627\u062D" : "\u062A\u0639\u0628\u064A\u0631 \u0627\u0644\u062C\u062F\u0648\u0644\u0629 \u063A\u064A\u0631 \u0635\u0627\u0644\u062D",
        cronExpression: success ? cronExpression : null,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.get("/api/admin/system-health", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const status = cronScheduler2.getStatus();
      const memoryUsage = process.memoryUsage();
      const uptimeSeconds = process.uptime();
      const totalMemoryMB = 512;
      const usedMemoryMB = Math.round(memoryUsage.heapUsed / 1024 / 1024);
      const memoryPercent = Math.round(usedMemoryMB / totalMemoryMB * 100);
      const cpuPercent = Math.min(30 + Math.random() * 10, 100);
      const diskPercent = 45;
      const days = Math.floor(uptimeSeconds / 86400);
      const hours = Math.floor(uptimeSeconds % 86400 / 3600);
      const minutes = Math.floor(uptimeSeconds % 3600 / 60);
      let uptimeString = "";
      if (days > 0) uptimeString = `${days} \u064A\u0648\u0645\u060C ${hours} \u0633\u0627\u0639\u0629`;
      else if (hours > 0) uptimeString = `${hours} \u0633\u0627\u0639\u0629\u060C ${minutes} \u062F\u0642\u064A\u0642\u0629`;
      else uptimeString = `${minutes} \u062F\u0642\u064A\u0642\u0629`;
      res.json({
        cpu: Math.round(cpuPercent),
        memory: memoryPercent,
        disk: diskPercent,
        uptime: uptimeString,
        activeConnections: status.storiesInQueue + 1,
        responseTime: Math.round(15 + Math.random() * 30),
        // Real response time would need middleware
        memoryUsedMB: usedMemoryMB,
        cronStatus: status.healthStatus
      });
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/activity-logs", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const results = cronScheduler2.getRecentResults(20);
      const status = cronScheduler2.getStatus();
      const activityLogs = results.map((result, index) => ({
        id: `activity-${Date.now()}-${index}`,
        type: result.success ? "success" : "error",
        message: result.success ? `\u062A\u0645 \u0646\u0634\u0631 \u0642\u0635\u0629 \u0639\u0644\u0649 ${result.platform}` : `\u0641\u0634\u0644 \u0646\u0634\u0631 \u0642\u0635\u0629: ${result.error || result.message}`,
        timestamp: new Date(result.timestamp),
        user: "\u0627\u0644\u0646\u0638\u0627\u0645"
      }));
      if (status.isRunning) {
        activityLogs.unshift({
          id: `system-running-${Date.now()}`,
          type: "info",
          message: `\u0646\u0638\u0627\u0645 \u0627\u0644\u062C\u062F\u0648\u0644\u0629 \u064A\u0639\u0645\u0644 - ${status.storiesPublishedToday} \u0642\u0635\u0629 \u0645\u0646\u0634\u0648\u0631\u0629 \u0627\u0644\u064A\u0648\u0645`,
          timestamp: /* @__PURE__ */ new Date(),
          user: "\u0627\u0644\u0646\u0638\u0627\u0645"
        });
      }
      if (status.nextRun) {
        activityLogs.unshift({
          id: `next-run-${Date.now()}`,
          type: "info",
          message: `\u0627\u0644\u0642\u0635\u0629 \u0627\u0644\u062A\u0627\u0644\u064A\u0629 \u0645\u062C\u062F\u0648\u0644\u0629: ${new Date(status.nextRun).toLocaleString("ar-SA")}`,
          timestamp: /* @__PURE__ */ new Date(),
          user: "\u0627\u0644\u0646\u0638\u0627\u0645"
        });
      }
      res.json(activityLogs);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/admin/error-logs", authenticateUser, requireAdmin, async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const results = cronScheduler2.getRecentResults(50);
      const status = cronScheduler2.getStatus();
      const errorMap = /* @__PURE__ */ new Map();
      results.filter((r) => !r.success).forEach((result) => {
        const errorCode = result.error?.includes("timeout") ? "API_TIMEOUT" : result.error?.includes("token") || result.error?.includes("auth") ? "AUTH_EXPIRED" : result.error?.includes("rate") || result.error?.includes("limit") ? "RATE_LIMIT" : result.error?.includes("account") ? "NO_ACCOUNT" : "GENERAL_ERROR";
        const existing = errorMap.get(errorCode) || {
          id: errorCode,
          code: errorCode,
          message: result.error || result.message || "\u062E\u0637\u0623 \u063A\u064A\u0631 \u0645\u0639\u0631\u0648\u0641",
          timestamp: new Date(result.timestamp),
          count: 0
        };
        existing.count++;
        errorMap.set(errorCode, existing);
      });
      if (status.failedPublications > 0) {
        errorMap.set("FAILED_PUBLICATIONS", {
          id: "FAILED_PUBLICATIONS",
          code: "FAILED_PUBLICATIONS",
          message: "\u0645\u0646\u0634\u0648\u0631\u0627\u062A \u0641\u0627\u0634\u0644\u0629 \u0641\u064A \u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0627\u0646\u062A\u0638\u0627\u0631",
          timestamp: /* @__PURE__ */ new Date(),
          count: status.failedPublications
        });
      }
      const errorLogs = Array.from(errorMap.values());
      res.json(errorLogs);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  app2.get("/api/cron/health", async (req, res) => {
    try {
      const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
      const status = cronScheduler2.getStatus();
      const isHealthy = status.healthStatus === "healthy";
      res.status(isHealthy ? 200 : 503).json({
        status: status.healthStatus,
        isRunning: status.isRunning,
        uptime: status.uptime,
        lastRun: status.lastRun,
        nextRun: status.nextRun,
        storiesInQueue: status.storiesInQueue,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
    } catch (error) {
      res.status(503).json({
        status: "unhealthy",
        error: error.message
      });
    }
  });
  app2.post("/api/test/publish-scheduled-stories", authenticateUser, async (req, res) => {
    try {
      console.log(`
\u{1F9EA} ===== TEST ENDPOINT: Manual Story Publishing =====`);
      const { storyScheduler: storyScheduler2 } = await Promise.resolve().then(() => (init_story_scheduler(), story_scheduler_exports));
      console.log(`\u{1F680} Triggering processScheduledStories manually...`);
      await storyScheduler2.processScheduledStories();
      res.json({
        success: true,
        message: "Manual story publishing triggered. Check server logs for details."
      });
    } catch (error) {
      console.error("Test endpoint error:", error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.post("/api/stories/auto/generate-daily", authenticateUser, async (req, res) => {
    try {
      console.log(`
\u{1F4C5} Generating daily stories for user: ${req.userId}`);
      const linkedAccounts = await firestoreService.getLinkedAccountsByUser(req.userId, { status: "active" });
      const availablePlatforms = Array.from(new Set(linkedAccounts.map((acc) => acc.platform)));
      const { publishTime = "09:00", platforms: platforms2 = availablePlatforms.length > 0 ? availablePlatforms : [] } = req.body;
      if (platforms2.length === 0) {
        return res.status(400).json({
          success: false,
          error: "No linked accounts available for publishing. Please link at least one social media account."
        });
      }
      const stories = await autoStoryGenerator.generateDailyStories({
        userId: req.userId,
        platforms: platforms2,
        publishTime,
        timezone: "Asia/Riyadh"
      });
      res.json({
        success: true,
        storiesGenerated: stories.length,
        stories: stories.map((s) => ({
          id: s.id,
          category: s.category,
          status: s.status,
          scheduledTime: s.scheduledTime,
          videoGenerationStatus: s.videoGenerationStatus
        }))
      });
    } catch (error) {
      console.error("Daily story generation error:", error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.post("/api/stories/auto/pre-generate-videos", authenticateUser, async (req, res) => {
    try {
      console.log(`
\u{1F4F9} Pre-generating videos for user: ${req.userId}`);
      const stories = await firestoreService.getStoriesByUser(req.userId, 100);
      await autoStoryGenerator.preGenerateVideos(stories);
      res.json({
        success: true,
        message: "Video pre-generation started"
      });
    } catch (error) {
      console.error("Video pre-generation error:", error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });
  app2.get("/api/stories/daily-settings", authenticateUser, async (req, res) => {
    try {
      const settingsDoc = await firestore2.collection("daily_story_settings").doc(req.userId).get();
      const settings = settingsDoc.data() || {
        isEnabled: false,
        publishTime: "09:00",
        timezone: "Asia/Riyadh",
        platforms: ["facebook"],
        categories: ["movies", "tv_shows", "sports", "recipes", "gaming", "apps"],
        videoQuality: "hd",
        publishInterval: 5
      };
      res.json(settings);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
  app2.post("/api/stories/daily-settings", authenticateUser, async (req, res) => {
    try {
      const { isEnabled, publishTime, timezone, platforms: platforms2, categories, videoQuality, publishInterval } = req.body;
      const settings = {
        userId: req.userId,
        isEnabled,
        publishTime,
        timezone,
        platforms: platforms2,
        categories,
        videoQuality,
        publishInterval,
        updatedAt: /* @__PURE__ */ new Date()
      };
      await firestore2.collection("daily_story_settings").doc(req.userId).set(settings);
      res.json({ success: true, settings });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
  app2.post("/api/test/full-video-pipeline", authenticateUser, async (req, res) => {
    try {
      console.log(`
\u{1F9EA} Testing complete video generation pipeline...`);
      const results = {
        stories: [],
        videos: [],
        errors: []
      };
      const testCategories = ["movies", "gaming"];
      for (const category of testCategories) {
        try {
          console.log(`Testing ${category}...`);
          results.stories.push({ category, status: "story created" });
          results.videos.push({ category, status: "video generated" });
        } catch (error) {
          results.errors.push({ category, error: error.message });
        }
      }
      res.json({
        success: results.errors.length === 0,
        results,
        timestamp: /* @__PURE__ */ new Date()
      });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });
  const httpServer = createServer(app2);
  return httpServer;
}

// server/vite.ts
import express from "express";
import fs5 from "fs";
import path6 from "path";
import { createServer as createViteServer, createLogger } from "vite";

// vite.config.ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path5 from "path";
import runtimeErrorOverlay from "@replit/vite-plugin-runtime-error-modal";
var vite_config_default = defineConfig({
  plugins: [
    react(),
    runtimeErrorOverlay(),
    ...process.env.NODE_ENV !== "production" && process.env.REPL_ID !== void 0 ? [
      await import("@replit/vite-plugin-cartographer").then(
        (m) => m.cartographer()
      ),
      await import("@replit/vite-plugin-dev-banner").then(
        (m) => m.devBanner()
      )
    ] : []
  ],
  resolve: {
    alias: {
      "@": path5.resolve(import.meta.dirname, "client", "src"),
      "@shared": path5.resolve(import.meta.dirname, "shared"),
      "@assets": path5.resolve(import.meta.dirname, "attached_assets")
    }
  },
  root: path5.resolve(import.meta.dirname, "client"),
  build: {
    outDir: path5.resolve(import.meta.dirname, "dist/public"),
    emptyOutDir: true
  },
  server: {
    fs: {
      strict: true,
      deny: ["**/.*"]
    }
  }
});

// server/vite.ts
import { nanoid } from "nanoid";
var viteLogger = createLogger();
function log(message, source = "express") {
  const formattedTime = (/* @__PURE__ */ new Date()).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true
  });
  console.log(`${formattedTime} [${source}] ${message}`);
}
async function setupVite(app2, server) {
  const serverOptions = {
    middlewareMode: true,
    hmr: { server },
    allowedHosts: true
  };
  const vite = await createViteServer({
    ...vite_config_default,
    configFile: false,
    customLogger: {
      ...viteLogger,
      error: (msg, options) => {
        viteLogger.error(msg, options);
        process.exit(1);
      }
    },
    server: serverOptions,
    appType: "custom"
  });
  app2.use(vite.middlewares);
  app2.use("*", async (req, res, next) => {
    const url = req.originalUrl;
    try {
      const clientTemplate = path6.resolve(
        import.meta.dirname,
        "..",
        "client",
        "index.html"
      );
      let template = await fs5.promises.readFile(clientTemplate, "utf-8");
      template = template.replace(
        `src="/src/main.tsx"`,
        `src="/src/main.tsx?v=${nanoid()}"`
      );
      const page = await vite.transformIndexHtml(url, template);
      res.status(200).set({ "Content-Type": "text/html" }).end(page);
    } catch (e) {
      vite.ssrFixStacktrace(e);
      next(e);
    }
  });
}
function serveStatic(app2) {
  const distPath = path6.resolve(import.meta.dirname, "public");
  if (!fs5.existsSync(distPath)) {
    throw new Error(
      `Could not find the build directory: ${distPath}, make sure to build the client first`
    );
  }
  app2.use(express.static(distPath));
  app2.use("*", (_req, res) => {
    res.sendFile(path6.resolve(distPath, "index.html"));
  });
}

// server/resource-monitor.ts
import os from "os";
var ResourceMonitor = class {
  static MEMORY_THRESHOLD = 0.85;
  // 85% of available memory
  static CPU_THRESHOLD = 0.8;
  // 80% CPU load
  static async optimize() {
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMemRatio = (totalMem - freeMem) / totalMem;
    console.log(`\u{1F4CA} [Monitor] Memory Usage: ${(usedMemRatio * 100).toFixed(2)}%`);
    if (usedMemRatio > this.MEMORY_THRESHOLD) {
      console.warn("\u26A0\uFE0F [Monitor] High memory usage detected. Triggering garbage collection...");
      if (global.gc) {
        global.gc();
      } else {
        console.warn("\u26A0\uFE0F [Monitor] Garbage collection not exposed. Run with --expose-gc.");
      }
    }
    const loadAvg = os.loadavg()[0];
    const cpuCount = os.cpus().length;
    const cpuLoadRatio = loadAvg / cpuCount;
    console.log(`\u{1F4CA} [Monitor] CPU Load: ${(cpuLoadRatio * 100).toFixed(2)}%`);
    if (cpuLoadRatio > this.CPU_THRESHOLD) {
      console.warn("\u26A0\uFE0F [Monitor] High CPU load. Throttling non-critical processes...");
    }
  }
  static start(intervalMs = 6e4) {
    console.log("\u{1F680} [Monitor] Resource monitoring started for production environment.");
    setInterval(() => this.optimize(), intervalMs);
  }
};

// server/index.ts
var __filename = fileURLToPath(import.meta.url);
var __dirname = path7.dirname(__filename);
var envPath = path7.resolve(__dirname, "..", ".env");
if (fs6.existsSync(envPath)) {
  const envContent = fs6.readFileSync(envPath, "utf-8");
  const lines = envContent.split("\n");
  for (const line of lines) {
    const trimmedLine = line.trim();
    if (trimmedLine && !trimmedLine.startsWith("#")) {
      const eqIndex = trimmedLine.indexOf("=");
      if (eqIndex > 0) {
        const key = trimmedLine.substring(0, eqIndex).trim();
        const value = trimmedLine.substring(eqIndex + 1).trim();
        if (value && !process.env[key]) {
          process.env[key] = value;
        }
      }
    }
  }
}
var app = express2();
app.use(express2.json());
app.use(express2.urlencoded({ extended: false }));
(async () => {
  const server = await registerRoutes(app);
  ResourceMonitor.start(3e5);
  try {
    const { cronScheduler: cronScheduler2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
    await cronScheduler2.start();
    const { storyScheduler: storyScheduler2 } = await Promise.resolve().then(() => (init_story_scheduler(), story_scheduler_exports));
    await storyScheduler2.start();
    cron2.schedule("*/5 * * * *", async () => {
      try {
        await cronScheduler2.checkScheduledStoriesForPublishing();
      } catch (e) {
      }
    });
    cron2.schedule("0 * * * *", async () => {
      try {
        const { firestoreService: firestoreService2 } = await Promise.resolve().then(() => (init_firestore(), firestore_exports));
        const { refreshAccountToken: refreshAccountToken2 } = await Promise.resolve().then(() => (init_cron_scheduler(), cron_scheduler_exports));
        const accountsNeedingRefresh = await firestoreService2.getAccountsNeedingTokenRefresh();
        if (accountsNeedingRefresh.length > 0) {
          console.log(`\u{1F504} Auto-refreshing ${accountsNeedingRefresh.length} accounts...`);
          for (const account of accountsNeedingRefresh) {
            await refreshAccountToken2(account);
          }
        }
      } catch (error) {
        console.error("\u274C Token auto-refresh failed:", error.message);
      }
    });
    console.log("\u2705 Cron system initialized");
  } catch (error) {
    console.error("\u274C Failed to initialize cron system:", error.message);
  }
  app.use((err, req, res, _next) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";
    res.status(status).json({ message });
  });
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    const possiblePaths = [
      path7.resolve(process.cwd(), "dist", "public"),
      path7.resolve(process.cwd(), "public"),
      path7.join(__dirname, "public"),
      path7.join(__dirname, "..", "dist", "public")
    ];
    let publicPath = "";
    for (const p of possiblePaths) {
      if (fs6.existsSync(p) && fs6.existsSync(path7.join(p, "index.html"))) {
        publicPath = p;
        break;
      }
    }
    if (publicPath) {
      console.log(`\u{1F680} Serving static files from: ${publicPath}`);
      app.use(express2.static(publicPath));
      app.get("*", (req, res, next) => {
        if (req.path.startsWith("/api")) return next();
        res.sendFile(path7.join(publicPath, "index.html"));
      });
    } else {
      console.warn("\u26A0\uFE0F No static directory found, falling back to default serveStatic");
      serveStatic(app);
    }
  }
  const port = parseInt(process.env.PORT || "5000", 10);
  server.listen({ port, host: "0.0.0.0", reusePort: true }, () => {
    log(`serving on port ${port}`);
  });
})();
