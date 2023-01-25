import { Request, Response } from "express";
import { handleResponseError } from "../route-handlers/route-error-handler";
import { Collection, ObjectId } from "mongodb";
import {
  ISegment,
  ISegmentGenderData,
  ISegmentMetaData,
} from "../../common/types/db-models/segment";
import { getDbWrapper } from "../../common/db/mongo-wrapper";

// export async function segmentList(req: Request, res: Response): Promise<void> {
//     try {
//     const segmentCollection: Collection = await (
//       await getDbWrapper()
//     ).getCollection("segments");
//     const usersCollection: Collection = await (
//       await getDbWrapper()
//     ).getCollection("users");
//     const metaData = await usersCollection.aggregate([
//       {
//         $lookup: {
//           from: "segments",
//           localField: "segment_ids",
//           foreignField: "_id",
//           as: "segments"
//         }
//       },
//       {
//         $group: {
//           _id: "$_id",
//           name: { $first: "$name" },
//           userCount: { $sum: { $size: "$users" } },
//           avgIncome: { $avg: "$users.income_level" },
//           topGender: { $first: "$users.gender" }
//         }
//       },
//       {
//         $project: {
//           _id: 0,
//           name: 1,
//           userCount: 1,
//           avgIncome: 1,
//           topGender: 1
//         }
//       }
//     ]).toArray();
//     const data = metaData.map((meta: any) => {
//       return {
//         name: meta.name,
//         userCount: meta.userCount,
//         avgIncome: meta.avgIncome,
//         topGender: meta.topGender
//       } as ISegmentMetaData;
//     });
//     const totalCount = await segmentCollection.countDocuments();
//     res.json({ success: true, data, totalCount });
//   } catch (error) {
//     handleResponseError(
//       `SegmentList Error: ${error.message}`,
//       error.message,
//       res
//     );
//   }
// }

// export async function segmentList(req: Request, res: Response): Promise<void> {
//     try {
//     const segmentCollection: Collection = await (
//       await getDbWrapper()
//     ).getCollection("segments");
//     const usersCollection: Collection = await (
//       await getDbWrapper()
//     ).getCollection("users");
//     const metaData = await segmentCollection.aggregate([
//       {
//         $lookup: {
//           from: "users",
//           localField: "_id",
//           foreignField: "segment_ids",
//           as: "users"
//         }
//       },
//       {
//         $group: {
//           _id: "$_id",
//           name: { $first: "$name" },
//           userCount: { $sum: { $size: "$users" } },
//           avgIncome: { $avg: "$users.income_level" },
//           topGender: { $first: "$users.gender" }
//         }
//       },
//       {
//         $project: {
//           _id: 0,
//           name: 1,
//           userCount: 1,
//           avgIncome: 1,
//           topGender: 1
//         }
//       }
//     ]).toArray();
//     const data = metaData.map((meta: any) => {
//       return {
//         name: meta.name,
//         userCount: meta.userCount,
//         avgIncome: meta.avgIncome,
//         topGender: meta.topGender
//       } as ISegmentMetaData;
//     });
//     const totalCount = await segmentCollection.countDocuments();
//     res.json({ success: true, data, totalCount });
//   } catch (error) {
//     handleResponseError(
//       `SegmentList Error: ${error.message}`,
//       error.message,
//       res
//     );
//   }
// }


export async function segmentList(req: Request, res: Response): Promise<void> {
  try {
    //test    
    const usersCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("users");

    var cursor = await usersCollection.find({}, {projection: {gender: 1, income_level: 1, income_type: 1, segment_ids: 1}}).limit(100);
    let segmentArray = [];
    while (await cursor.hasNext()) {
        let batch = await cursor.next();
        batch.segment_ids.forEach(segment_id => {
          if(!segmentArray.find(segment => segment.segment_id.toString() === segment_id.toString())) {
            segmentArray.push({name: "", segment_id: segment_id, genderFemale: batch.gender == "Female" ? 1 : 0, genderMale: batch.gender == "Male" ? 1 : 0, income_level: batch.income_type == "yearly" ? batch.income_level / 12 : batch.income_level, userCount: 1})
          }
          else {
          var foundSegment = segmentArray.find(x => x.segment_id = segment_id);
          var mounthlyUserIncome = batch.income_type == "yearly" ? batch.income_level / 12 : batch.income_level;
          foundSegment.income_level = (foundSegment.income_level + mounthlyUserIncome) / foundSegment.userCount;
          foundSegment.genderFemale = batch.gender == "Female" ? foundSegment.genderFemale+1 : foundSegment.genderFemale;
          foundSegment.genderMale = batch.gender == "Male" ? foundSegment.genderMale++ : foundSegment.genderMale;
          }
        });
    }
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");

    var arrayToSend = [];
      for (const segment of segmentArray) {
        const segmentObjectToFetchName = await segmentCollection.findOne({ _id: segment.segment_id }, { projection: { name: 1 } });
        var dominantGender;
        if(segment.genderFemale > segment.genderMale) dominantGender = "Female";
        else if(segment.genderMale > segment.genderFemale) dominantGender = "Male";
        else dominantGender = "Equal";
        var name = ''
        if(segmentObjectToFetchName && segmentObjectToFetchName.name)
        {
          name = segmentObjectToFetchName.name;
        }
        arrayToSend.push({_id: segment.segment_id, name: name, userCount: segment.userCount, avgIncome: Math.round(segment.income_level), topGender: dominantGender})
    }

    const totalCount = await segmentCollection.countDocuments();
    res.json({ success: true, data: arrayToSend, totalCount });
  } catch (error) {
    handleResponseError(
      `SegmentList Error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");
    const segment: ISegment = await segmentCollection.findOne({
      _id: new ObjectId(req.params.id as string),
    });
    if (!segment) {
      return handleResponseError(
        `Error getSegmentById`,
        `Segment with id ${req.params.id} not found.`,
        res
      );
    }
    res.json({ success: true, data: segment });
  } catch (error) {
    handleResponseError(
      `Get Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function updateSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    // res.json({ success: true });
  } catch (error) {
    handleResponseError(
      `Update Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentGenderData(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");
      const userCollection: Collection = await (await getDbWrapper()).getCollection("users");
      const genderData = await userCollection.aggregate([
          {
              $group: {
                  _id: "$gender",
                  userCount: { $sum: 1 }
              }
          }
      ]).toArray();
      const totalUsers = await userCollection.countDocuments();
      const data = genderData.map(g => {
          return {
              _id: g._id,
              userCount: g.userCount,
              userPercentage: (g.userCount / totalUsers) * 100
          } as ISegmentGenderData
      });
      res.json({ success: true, data });
  } catch (error) {
    handleResponseError(
      `Segment gender data error: ${error.message}`,
      error.message,
      res
    );
  }
}
