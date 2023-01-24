import { Request, Response } from "express";
import { handleResponseError } from "../route-handlers/route-error-handler";
import { Collection, ObjectId } from "mongodb";
import {
  ISegment,
  ISegmentGenderData,
  ISegmentMetaData,
} from "../../common/types/db-models/segment";
import { getDbWrapper } from "../../common/db/mongo-wrapper";

export async function segmentList(req: Request, res: Response): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");
    const metaData = await segmentCollection.find({}, { projection: { name: 1} }).toArray();
    const totalCount = await segmentCollection.countDocuments();
    res.json({ success: true, data: metaData, totalCount });
  } catch (error) {
    handleResponseError(
      `Get Segment List Error: ${error.message}`,
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
          }
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
