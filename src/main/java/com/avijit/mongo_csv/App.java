package com.avijit.mongo_csv;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

/**
 * Hello world!
 *
 */
public class App {
	private static final String csvFileName = System.getProperty("user.home") + "/student.csv";

	// CSV file header
	private static final String[] FILE_HEADER_MAPPING = { "id", "firstName", "lastName", "gender", "age" };

	// Student attributes
	private static final String STUDENT_ID = "id";
	private static final String STUDENT_FNAME = "firstName";
	private static final String STUDENT_LNAME = "lastName";
	private static final String STUDENT_GENDER = "gender";
	private static final String STUDENT_AGE = "age";

	// Mongodb attibutes
	private static final String hostname = "localhost";
	private static final int port = 27017;
	private static final String dbName = "test-mongo";
	private static final String collectionName = "student-collection";

	public static void main(String[] args) {
		FileReader fileReader = null;
		CSVParser csvFileParser = null;
		MongoClient mongoClient = null;
		try {
			// ---------- Connecting DataBase -------------------------//
			mongoClient = new MongoClient(hostname, port);
			// ---------- Creating DataBase ---------------------------//
			MongoDatabase db = mongoClient.getDatabase(dbName);
			// ---------- Creating Collection -------------------------//
			MongoCollection<Document> table = db.getCollection(collectionName);

			// Create the CSVFormat object with the header mapping
			CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

			// Create a new list of student to be filled by CSV file data
			List<Student> students = new ArrayList<Student>();

			// initialize FileReader object
			fileReader = new FileReader(csvFileName);

			// initialize CSVParser object
			csvFileParser = new CSVParser(fileReader, csvFileFormat);

			// Get a list of CSV file records
			List<CSVRecord> csvRecords = csvFileParser.getRecords();

			// Read the CSV file records starting from the second record to skip the header
			for (int i = 1; i < csvRecords.size(); i++) {
				CSVRecord record = csvRecords.get(i);
				// Create a new student object and fill his data
				Student student = new Student(Long.parseLong(record.get(STUDENT_ID)), record.get(STUDENT_FNAME),
						record.get(STUDENT_LNAME), record.get(STUDENT_GENDER),
						Integer.parseInt(record.get(STUDENT_AGE)));
				students.add(student);
			}

			// Print the new student list
			for (Student student : students) {
				// ---------- Creating Document ---------------------------//
				Document doc = new Document(STUDENT_ID, student.getId());
				doc.append(STUDENT_FNAME, student.getFirstName());
				doc.append(STUDENT_LNAME, student.getFirstName());
				doc.append(STUDENT_GENDER, student.getGender());
				doc.append(STUDENT_AGE, student.getAge());
				// ----------- Inserting Data ------------------------------//
				table.insertOne(doc);

				System.out.println(student.toString());
			}
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			try {
				fileReader.close();
				csvFileParser.close();
				mongoClient.close();
			} catch (IOException e) {
				System.out.println("Error while closing fileReader/csvFileParser !!!");
				e.printStackTrace();
			}
		}
	}
}
