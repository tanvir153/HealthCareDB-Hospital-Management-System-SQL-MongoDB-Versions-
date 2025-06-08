from pymongo import MongoClient, ASCENDING
import random
import datetime

# MongoDB URI & client setup
uri = "mongodb+srv://tanvirxf:7T0YMWUdLEjVXWtl@cluster0.uakz7xn.mongodb.net/?retryWrites=true&w=majority&ssl=true&authSource=admin"
client = MongoClient(uri)
db = client["MediCareDB"]

try:
    client.admin.command('ping')
    print("Connected to MongoDB Atlas.")
except Exception as e:
    print("Connection failed:", e)
    exit()

# ---------------- Generate Data---------------

def generate_patient_info(id):
    return {
        "PatientID": id,
        "FirstName": f"PatientFirst{id}",
        "LastName": f"Last{id}",
        "Age": random.randint(20, 85),
        "DOB": f"{random.randint(1,12):02}/{random.randint(1,28):02}/{random.randint(1960,2000)}",
        "Gender": random.choice(["Male", "Female"]),
        "ContactNumber": f"+1-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "Address": f"{random.randint(100,999)} {random.choice(['Main St','Oak St','Pine St'])}",
        "BloodType": random.choice(["A+", "B+", "O+", "AB+"]),
        "Allergies": random.choice(["None", "Peanuts", "Pollen", "Shellfish"]),
        "MedicalHistory": random.choice(["None", "Diabetes", "Hypertension", "Asthma"])
    }

def generate_doctor(id):
    return {
        "DoctorID": id,
        "FirstName": f"DoctorFirst{id}",
        "LastName": f"DoctorLast{id}",
        "Specialization": random.choice(["Cardiology", "Neurology", "Pediatrics"]),
        "ContactNumber": f"+1-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "Email": f"doctor{id}@hospital.com",
        "DepartmentID": random.randint(1, 10),
        "HospitalID": 1
    }

def generate_appointment(pid, doc):
    return {
        "PatientID": pid["PatientID"],
        "DoctorID": doc["DoctorID"],
        "PatientName": pid["FirstName"] + " " + pid["LastName"],  # Denormalized Patient's Name
        "DoctorName": doc["FirstName"] + " " + doc["LastName"],    # Denormalized Doctor's Name
        "Date": datetime.datetime(2024, random.randint(1, 12), random.randint(1, 28), random.randint(9, 17)),
        "Status": random.choice(["Scheduled", "Completed"]),
        "Notes": "Follow-up"
    }

def generate_billing(pid):
    return {
        "PatientID": pid["PatientID"],
        "PatientName": pid["FirstName"] + " " + pid["LastName"],  # Denormalized Patient's name again
        "TotalAmount": round(random.uniform(50, 500), 2),
        "PaymentStatus": random.choice(["Paid", "Pending", "Overdue"])
    }

def generate_prescription(pid):
    return {
        "PatientID": pid["PatientID"],
        "Medication": random.choice(["Ibuprofen", "Paracetamol"]),
        "Dosage": "1 tablet",
        "Duration": "5 days"
    }

def generate_lab_result(pid):
    return {
        "PatientID": pid["PatientID"],
        "TestName": random.choice(["Blood Test", "X-Ray"]),
        "Result": random.choice(["Normal", "Abnormal"]),
        "ResultDate": str(datetime.datetime.now().date())
    }

def generate_room(pid):
    return {
        "PatientID": pid["PatientID"],
        "RoomNumber": f"R{random.randint(100,999)}",
        "AdmissionDate": str(datetime.datetime.now().date()),
        "DischargeDate": str(datetime.datetime.now().date())
    }

def generate_pharmacy():
    return {
        "DrugName": random.choice(["Aspirin", "Amoxicillin"]),
        "Manufacturer": random.choice(["PharmaX", "MedCorp"]),
        "ExpiryDate": str(datetime.datetime(2025, random.randint(1, 12), 1)),
        "Stock": random.randint(10, 500)
    }

def generate_hospital():
    return {
        "HospitalID": 1,
        "HospitalName": "General Hospital",
        "Location": "London",
        "PhoneNumber": "+44-1234-567890",
        "Email": "info@hospital.com"
    }

# ---------------- Generate Bulk Datasets ----------------

patients = [generate_patient_info(i) for i in range(1, 201)]
doctors = [generate_doctor(i) for i in range(1, 21)]
appointments = [generate_appointment(random.choice(patients), random.choice(doctors)) for _ in range(300)]
billings = [generate_billing(random.choice(patients)) for _ in range(300)]
prescriptions = [generate_prescription(random.choice(patients)) for _ in range(200)]
labs = [generate_lab_result(random.choice(patients)) for _ in range(200)]
rooms = [generate_room(random.choice(patients)) for _ in range(200)]
pharmacies = [generate_pharmacy() for _ in range(20)]
departments = [{"DepartmentID": i, "DepartmentName": f"Dept{i}"} for i in range(1, 11)]
hospitals = [generate_hospital()]

#----------------------Bulk Insertation to Corresponding MongoDB Collections------------

db["patients"].insert_many(patients)
db["doctors"].insert_many(doctors)
db["appointments"].insert_many(appointments)
db["billing_records"].insert_many(billings)
db["prescriptions"].insert_many(prescriptions)
db["lab_results"].insert_many(labs)
db["room_assignments"].insert_many(rooms)
db["pharmacy"].insert_many(pharmacies)
db["departments"].insert_many(departments)
db["hospitals"].insert_many(hospitals)

# ---------------- Indexing ----------------

db["patients"].create_index([("PatientID", ASCENDING)])
db["appointments"].create_index([("DoctorID", ASCENDING)])
db["appointments"].create_index([("PatientID", ASCENDING)])
db["billing_records"].create_index([("PatientID", ASCENDING)])

# ---------------- Queries with Execution time and Aggregation ----------------

# Q1. Find patients who are aged over 60
print("Q1 - Patients over age 60")
explain_cmd = {"find": "patients", "filter": {"Age": {"$gt": 60}}}
stats = db.command("explain", explain_cmd, verbosity="executionStats")  # Measures query execution performance
print(f"executionTimeMillis: {stats['executionStats']['executionTimeMillis']} ms")  # Prints execution time
for patient in db.patients.find({"Age": {"$gt": 60}}, {"_id": 0, "PatientID": 1, "FirstName": 1, "Age": 1}):
    print(patient)
print()

# Q2. Find appointments by Doctor ID that is sorted by Date
print("Q2 - Appointments for DoctorID=101 sorted by Date")
explain_cmd = {
    "find": "appointments",
    "filter": {"DoctorID": 101},
    "sort": {"Date": -1}
}
stats = db.command("explain", explain_cmd, verbosity="executionStats")  # Measures query execution performance
print(f"executionTimeMillis: {stats['executionStats']['executionTimeMillis']} ms")  # Prints execution time
for appt in db.appointments.find({"DoctorID": 101}, {"_id": 0, "DoctorID": 1, "Date": 1}).sort("Date", -1):
    print(appt)
print()

# Q3. Count how many prescriptions are for 'Paracetamol'
print("Q3 - Count of 'Paracetamol' prescriptions")
stats = db.command({
    "explain": {
        "count": "prescriptions",
        "query": {"Medication": "Paracetamol"}
    },
    "verbosity": "executionStats"
})  # Measures performance of count query
print(f"executionTimeMillis: {stats['executionStats']['executionTimeMillis']} ms")  # Prints execution time
count = db.prescriptions.count_documents({"Medication": "Paracetamol"})
print(f"Total prescriptions for Paracetamol: {count}\n")

# Q4. Aggregate: Appointments grouped by department
print("Q4 - Appointments per Department")
pipeline = [
    {"$lookup": {  # Join with doctors collection
        "from": "doctors",
        "localField": "DoctorID",
        "foreignField": "DoctorID",
        "as": "doctor_info"
    }},
    {"$unwind": "$doctor_info"},  # Flatten array from $lookup
    {"$group": {  # Group by department ID
        "_id": "$doctor_info.DepartmentID",
        "TotalAppointments": {"$sum": 1}
    }}
]
stats = db.command({
    "explain": {
        "aggregate": "appointments",
        "pipeline": pipeline,
        "cursor": {}
    },
    "verbosity": "executionStats"
})  # Measures aggregation execution performance
execution_time = stats['stages'][0]['$cursor']['executionStats']['executionTimeMillis']
print(f"executionTimeMillis: {execution_time} ms")
for result in db.appointments.aggregate(pipeline):
    print(result)
print()

# Q5. Find doctors who have more than 10 patients assigned
print("Q5 - Doctors with >10 patients")
pipeline = [
    {"$group": {  # Group by doctor ID and count patients
        "_id": "$DoctorID",
        "TotalPatients": {"$sum": 1}
    }},
    {"$match": {"TotalPatients": {"$gt": 10}}},  # Only include those with more than 10
    {"$sort": {"TotalPatients": -1}}  # Sort descending
]
stats = db.command({
    "explain": {
        "aggregate": "patients",
        "pipeline": pipeline,
        "cursor": {}
    },
    "verbosity": "executionStats"
})  # Measures performance of aggregation
execution_time = stats['stages'][0]['$cursor']['executionStats']['executionTimeMillis']
print(f"executionTimeMillis: {execution_time} ms")
for result in db.patients.aggregate(pipeline):
    print(result)
print()

# Q6. Total billing amount per patient
print("Q6 - Total billed per patient")
aggregation_query = [
    {"$group": {
        "_id": "$PatientID",
        "totalBilled": {"$sum": "$TotalAmount"}
    }}
]
for doc in db.billing_records.aggregate(aggregation_query):
    print(doc)

# Q7. Total number of appointments per doctor
print("\nQ7 - Appointments per doctor")
aggregation_query = [
    {"$group": {
        "_id": "$DoctorID",
        "appointmentCount": {"$sum": 1}
    }}
]
for doc in db.appointments.aggregate(aggregation_query):
    print(doc)

# Q8. Total number of prescriptions per patient
print("\nQ8 - Prescriptions per patient")
aggregation_query = [
    {"$group": {
        "_id": "$PatientID",
        "prescriptionCount": {"$sum": 1}
    }}
]
for doc in db.prescriptions.aggregate(aggregation_query):
    print(doc)

# Q9. Top 5 most frequently prescribed medications
print("\nQ9 - Top 5 most common medications")
aggregation_query = [
    {"$group": {
        "_id": "$Medication",
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},  # Sort by count descending
    {"$limit": 5}  # Limit to top 5
]
for doc in db.prescriptions.aggregate(aggregation_query):
    print(doc)

print("\nAll operations complete.")
