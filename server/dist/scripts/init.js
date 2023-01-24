"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const mongo_wrapper_1 = require("../common/db/mongo-wrapper");
const user_1 = require("../common/types/db-models/user");
function cleanCollections() {
    return __awaiter(this, void 0, void 0, function* () {
        const segmentsCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("segments");
        const usersCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("users");
        console.log("cleaning segment collection");
        yield segmentsCollection.deleteMany({});
        console.log("cleaning user collection");
        yield usersCollection.deleteMany({});
        console.log("Done cleaning");
    });
}
function randomNumber(min, max) {
    return Math.round(Math.random() * (max - min) + min);
}
function createUsers() {
    return __awaiter(this, void 0, void 0, function* () {
        const incomes = [
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 5000,
                type: "monthly",
            },
            {
                value: 7500,
                type: "monthly",
            },
            {
                value: 10000,
                type: "monthly",
            },
            {
                value: 15000,
                type: "monthly",
            },
            {
                value: 10000,
                type: "monthly",
            },
            {
                value: 15000,
                type: "monthly",
            },
            {
                value: 10000,
                type: "monthly",
            },
            {
                value: 15000,
                type: "monthly",
            },
            {
                value: 10000,
                type: "monthly",
            },
            {
                value: 15000,
                type: "monthly",
            },
            {
                value: 25000,
                type: "monthly",
            },
            {
                value: 25000,
                type: "monthly",
            },
            {
                value: 35000,
                type: "monthly",
            },
            {
                value: 35000,
                type: "monthly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 75000,
                type: "yearly",
            },
            {
                value: 100000,
                type: "yearly",
            },
            {
                value: 150000,
                type: "yearly",
            },
            {
                value: 150000,
                type: "yearly",
            },
            {
                value: 150000,
                type: "yearly",
            },
            {
                value: 150000,
                type: "yearly",
            },
            {
                value: 150000,
                type: "yearly",
            },
            {
                value: 250000,
                type: "yearly",
            },
            {
                value: 250000,
                type: "yearly",
            },
            {
                value: 500000,
                type: "yearly",
            },
        ];
        const genders = [
            "Female",
            "Male",
            "Female",
            "Female",
            "Female",
            "Female",
            "Female",
            "Male",
            "Male",
            "Male",
            "Male",
            "Male",
            "Female",
            "Female",
            "Female",
            "Female",
            "Female",
            "Female",
            "Male",
            "Male",
            "Male",
            "Male",
            "Male",
            "Male",
            "Other",
        ];
        const genderLen = genders.length - 1;
        const incomeLen = incomes.length - 1;
        const usersCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("users");
        const totalUsers = 100000;
        for (let i = 0; i < totalUsers; i++) {
            if (i % 1000 === 0) {
                console.log(`starting ${i}/${totalUsers}`);
            }
            const gender = randomNumber(0, genderLen);
            const income = randomNumber(0, incomeLen);
            const age = randomNumber(18, 65);
            const user = {
                age: age,
                gender: genders[gender],
                income_level: incomes[income].value,
                income_type: incomes[income].type,
            };
            yield usersCollection.insertOne(user);
        }
    });
}
function createSegments() {
    return __awaiter(this, void 0, void 0, function* () {
        const qualities = [
            "Smart",
            "Ambitious",
            "Lazy",
            "Stupid",
            "Fat",
            "Skinny",
            "Dexterous",
            "Nimble",
            "Agile",
            "Sincere",
            "Honest",
            "Understanding",
            "Loyal",
            "Truthful",
            "Trustworthy",
            "Intelligent",
            "Dependable",
            "Open-Minded",
            "Wise",
            "Reliable",
            "Mature",
            "Friendly",
            "King-Hearted",
            "Warm",
            "Happy",
            "Good-Natured",
            "Selfish",
            "Honorable",
            "Respectful",
            "Cheerful",
            "Responsible",
            "Angry",
            "Furious",
            "Evil",
            "Helpful",
            "Brilliant",
        ];
        const groups = [
            "Kids",
            "Teenagers",
            "Adults",
            "People",
            "Women",
            "Investigators",
            "Men",
            "Detectives",
            "Chef's",
            "Cooks",
            "Policemen",
            "Firefighters",
            "Politicians",
            "Nanny's",
            "Mommies",
            "Daddies",
            "Grandparents",
            "Aunts & Uncles",
            "Retailers",
            "Developers",
            "Cleaners",
        ];
        const groupLen = groups.length - 1;
        const qualitiesLen = qualities.length - 1;
        const segmentsCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("segments");
        const usersCollection = yield (yield (0, mongo_wrapper_1.getDbWrapper)()).getCollection("users");
        const totalSegments = 2500;
        const maxSegmentSize = 5000;
        const nameSet = new Set();
        for (let i = 0; i < totalSegments; i++) {
            if (i % 100 === 0) {
                console.log(`starting ${i}/${totalSegments}`);
            }
            const groupIndex = randomNumber(0, groupLen - 1);
            const qualityIndex = randomNumber(0, qualitiesLen - 1);
            let segmentName = `${qualities[qualityIndex]} ${groups[groupIndex]}`;
            while (nameSet.has(segmentName)) {
                const qualityIndex2 = randomNumber(0, qualitiesLen - 1);
                segmentName = `${qualities[qualityIndex2]} ${segmentName}`;
            }
            nameSet.add(segmentName);
            const inserted = yield segmentsCollection.insertOne({
                name: segmentName,
            });
            const segmentSize = randomNumber(0, maxSegmentSize);
            const userSelectionPipeline = [
                {
                    $sample: {
                        size: segmentSize,
                    },
                },
                {
                    $project: {
                        _id: 1,
                    },
                },
            ];
            if (segmentName.endsWith("Daddies") || segmentName.endsWith("Men")) {
                userSelectionPipeline.unshift({
                    $match: {
                        gender: user_1.Gender.Male,
                    },
                });
            }
            if (segmentName.endsWith("Mommies") || segmentName.endsWith("Women")) {
                userSelectionPipeline.unshift({
                    $match: {
                        gender: user_1.Gender.Female,
                    },
                });
            }
            const users = yield usersCollection
                .aggregate(userSelectionPipeline)
                .toArray();
            yield usersCollection.updateMany({
                _id: { $in: users.map((v) => v._id) },
            }, {
                $addToSet: {
                    segment_ids: inserted.insertedId,
                },
            });
            console.log(`found ${users.length} users for ${segmentName}`);
        }
    });
}
(0, mongo_wrapper_1.connectMongoConnector)().then(() => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // await cleanCollections();
        console.log(`creating users`);
        yield createUsers();
        console.log(`creating users`);
        yield createSegments();
    }
    catch (e) {
        console.log((e === null || e === void 0 ? void 0 : e.message) || e);
    }
}));
//# sourceMappingURL=init.js.map