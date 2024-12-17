import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";
import { Client as HubSpotClient } from "@hubspot/api-client";
import { parse } from "csv-parse";
import { Readable } from "stream";
import * as dotenv from "dotenv";

dotenv.config();

const AWS_REGION = process.env.AWS_REGION!;
const HUBSPOT_SECRET_NAME = "hubspot-secret-name";

const s3Client = new S3Client({ region: AWS_REGION });
const secretsManagerClient = new SecretsManagerClient({ region: AWS_REGION });

/**
 * Retrieves the HubSpot access token from the Secrets Manager secret
 * with the given name. If the secret does not exist or does not contain
 * the access token, throws an error.
 *
 * @returns The HubSpot access token
 */
const getHubSpotAccessToken = async (): Promise<string> => {
  const command = new GetSecretValueCommand({
    SecretId: HUBSPOT_SECRET_NAME,
  });
  const response = await secretsManagerClient.send(command);

  if (!response.SecretString) {
    throw new Error("HubSpot access token not found in Secrets Manager");
  }
  return JSON.parse(response.SecretString).hubspotAccessToken;
};

/**
 * Reads a CSV file from S3, parses the stream to JSON, and returns the parsed data.
 *
 * @param {string} bucketName - The name of the S3 bucket containing the CSV file.
 * @param {string} objectKey - The key of the CSV file in S3.
 * @returns {Promise<any[]>} - A promise that resolves with the parsed JSON data.
 * @throws {Error} - Will throw an error if the file is not found, or if there is an
 * error processing the file.
 */
const readCsvFileFromS3 = async (
  bucketName: string,
  objectKey: string
): Promise<any[]> => {
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: objectKey,
  });
  const { Body } = await s3Client.send(command);

  if (!Body || !(Body instanceof Readable)) {
    throw new Error(
      `Unable to read CSV file '${objectKey}' from S3 bucket '${bucketName}'.`
    );
  }

  const contacts = [];
  const parser = Body.pipe(parse({ columns: true }));
  for await (const record of parser) {
    if (!record) {
      continue;
    }
    contacts.push(record);
  }
  return contacts;
};

/**
 * Processes an array of CSV contacts and returns an array of HubSpot contact objects
 * compatible with the HubSpot API.
 *
 * @param {any[]} contacts - The array of CSV contacts to process.
 * @returns {any[]} - A mapped array of HubSpot contact objects.

 */
const processCsvDataToPayload = (contacts: any[]): any[] => {
  return contacts.map((contact) => {
    // Filter out keys starting with an underscore in the `contact` object
    const properties = Object.keys(contact).reduce((acc, key) => {
      if (!key.startsWith("_")) {
        acc[key] = contact[key];
      }

      return acc;
    }, {} as Record<string, unknown>);

    return {
      id: properties.email,
      idProperty: "email",
      properties,
    };
  });
};

/**
 * Upserts the given array of HubSpot contact objects to HubSpot using the
 * `crm.contacts.batchApi.upsert` endpoint.
 *
 * @param {any[]} payloadContacts - An array of HubSpot contact objects to upsert.
 * @returns {Promise<void>} - A promise that resolves when the upsert operation is
 * complete.
 * @throws {Error} - Will throw an error if there is an error during the upsert
 * operation.
 */
const upsertContactsToHubSpot = async (
  payloadContacts: any[]
): Promise<void> => {
  const payload = {
    inputs: payloadContacts,
  };

  // Retrieve HubSpot access token
  const hubSpotAccessToken = await getHubSpotAccessToken();

  // Initialize HubSpot client
  const hubSpotClient: HubSpotClient = new HubSpotClient({
    accessToken: hubSpotAccessToken,
  });

  try {
    const response = await hubSpotClient.crm.contacts.batchApi.upsert(payload);
    console.log("HubSpot upsert response:", response);
  } catch (error) {
    console.error("Error during HubSpot upsert:", error);
    throw error;
  }
};

/**
 * Processes an SNS event containing an S3 bucket and key, reads the
 * associated CSV file, and upserts the data to HubSpot using the
 * `crm.contacts.batchApi.upsert` endpoint.
 *
 * @param {{
 *   Records: { Sns: { Message: { bucket: string; key: string } } }[];
 * }} event - The SNS event to process.
 *
 * @returns {Promise<void>} - A promise that resolves when the upsert
 * operation is complete.
 *
 * @throws {Error} - Will throw an error if there is an error during the
 * upsert operation.
 */
export const handler = async (event: {
  Records: { Sns: { Message: { bucket: string; key: string } } }[];
}): Promise<void> => {
  try {
    if (
      event === null ||
      event.Records === null ||
      event.Records === undefined ||
      event.Records.length === 0
    ) {
      throw new Error("No SNS records found in event");
    }

    const { bucket, key } = event.Records[0].Sns.Message;

    if (!bucket || !key) {
      throw new Error("S3 bucket or key not provided in SNS message");
    }

    console.log(`Bucket=${bucket}, Key=${key} | Processing CSV file...`);

    // Read and parse CSV data
    const csvData = await readCsvFileFromS3(bucket, key);

    if (!csvData || !csvData.length) {
      console.log(
        `Bucket=${bucket}, Key=${key} | No valid contacts to process for upsert to hubspot `
      );
      return;
    }

    // Process CSV Data for payload
    const payloadContacts = processCsvDataToPayload(csvData);

    if (payloadContacts && !payloadContacts.length) {
      console.log(`Bucket=${bucket}, Key=${key} | No valid contacts to upsert`);
      return;
    }
    console.debug("Payload contacts:", payloadContacts);

    // Upsert data to HubSpot
    await upsertContactsToHubSpot(payloadContacts);

    console.log(
      `Bucket=${bucket}, Key=${key} | Lambda execution completed successfully with upsert`
    );
  } catch (error) {
    console.error("Error in Lambda execution:", error);
    throw error;
  }
};
