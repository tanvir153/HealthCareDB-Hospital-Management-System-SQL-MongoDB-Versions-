-- ================================================================
-- Project: HealthCareDB_Unique - SQL Database Design and Build
-- Course: CO7401 - SQL Databases Design and Build
-- Student: J119811
-- University of Chester
-- ================================================================

-- =========================SET UP=======================================
SET STATISTICS TIME ON;
SET NOCOUNT ON;
USE master;
GO
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'HealthCareDB_Unique')
BEGIN
    ALTER DATABASE HealthCareDB_Unique SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE HealthCareDB_Unique;
END;
GO
CREATE DATABASE HealthCareDB_Unique;
GO
USE HealthCareDB_Unique;
GO

-- =================================TABLE CREATION===============================
CREATE TABLE Hospital (
    HospitalID INT PRIMARY KEY IDENTITY(1,1),
    Name VARCHAR(100) NOT NULL,
    Address TEXT NOT NULL,
    PhoneNumber VARCHAR(20),
    Email VARCHAR(100)
);

CREATE TABLE PatientInfo (
    PatientID INT PRIMARY KEY IDENTITY(1,1),
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    DOB DATE NOT NULL,
    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female', 'Other')),
    ContactNumber VARCHAR(15) UNIQUE,
    Address TEXT,
    BloodType VARCHAR(5),
    Allergies TEXT,
    MedicalHistory TEXT
);

CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY IDENTITY(1,1),
    DepartmentName VARCHAR(100) UNIQUE,
    Floor INT
);

CREATE TABLE DoctorDirectory (
    DoctorID INT PRIMARY KEY IDENTITY(1,1),
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    Specialization VARCHAR(100) NOT NULL,
    ContactNumber VARCHAR(15) UNIQUE,
    Email VARCHAR(100) UNIQUE NOT NULL,
    HospitalID INT NOT NULL FOREIGN KEY REFERENCES Hospital(HospitalID),
    DepartmentID INT NOT NULL FOREIGN KEY REFERENCES Departments(DepartmentID)
);

CREATE TABLE AppointmentSchedule (
    AppointmentID INT PRIMARY KEY IDENTITY(1,1),
    PatientID INT NOT NULL FOREIGN KEY REFERENCES PatientInfo(PatientID),
    DoctorID INT NOT NULL FOREIGN KEY REFERENCES DoctorDirectory(DoctorID),
    AppointmentDateTime DATETIME NOT NULL,
    Status VARCHAR(20) CHECK (Status IN ('Scheduled', 'Completed', 'Cancelled')),
    Notes TEXT
);

CREATE TABLE BillingRecords (
    BillID INT PRIMARY KEY IDENTITY(1,1),
    PatientID INT NOT NULL FOREIGN KEY REFERENCES PatientInfo(PatientID),
    TotalAmount DECIMAL(10,2) NOT NULL,
    PaymentStatus VARCHAR(20) CHECK (PaymentStatus IN ('Paid', 'Pending', 'Cancelled')),
    DateIssued DATE DEFAULT GETDATE()
);

CREATE TABLE Pharmacy(
    DrugID INT PRIMARY KEY IDENTITY(1,1),
    DrugName VARCHAR(100) NOT NULL,
    Manufacturer VARCHAR(100),
    ExpiryDate DATE NOT NULL,
    Stock INT CHECK (Stock >= 0)
);

CREATE TABLE LabResults (
    LabResultID INT PRIMARY KEY IDENTITY(1,1),
    PatientID INT NOT NULL FOREIGN KEY REFERENCES PatientInfo(PatientID),
    TestName VARCHAR(100),
    Result TEXT,
    ResultDate DATE
);

CREATE TABLE RoomAssignments (
    AssignmentID INT PRIMARY KEY IDENTITY(1,1),
    PatientID INT NOT NULL FOREIGN KEY REFERENCES PatientInfo(PatientID),
    RoomNumber VARCHAR(10),
    AdmissionDate DATE,
    DischargeDate DATE
);

CREATE TABLE Prescription (
    PrescriptionID INT PRIMARY KEY IDENTITY(1,1),
    PatientID INT NOT NULL FOREIGN KEY REFERENCES PatientInfo(PatientID),
    Medication VARCHAR(100) NOT NULL,
    Dosage VARCHAR(100),
    Duration VARCHAR(100)
);

-- ==========================DATA INSERTION======================================


-- Insert Departments 
INSERT INTO Departments (DepartmentName, Floor)
VALUES 
('Cardiology', 1),
('Neurology', 2),
('General Medicine', 3),
('Pediatrics', 4),
('Radiology', 5);

-- Insert Hospitals
-- Code adapted from Microsoft Docs WHILE Loop Example (n.d.-a)
DECLARE @i INT;
SET @i = 1;
WHILE @i <= 5
BEGIN
    INSERT INTO Hospital (Name, Address, PhoneNumber, Email)
    VALUES (
        CONCAT('Health Hospital ', @i),
        CONCAT(@i, ' Main Street, City'),
        CONCAT('0170000000', @i),
        CONCAT('hospital', @i, '@healthcare.com')
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Patients
-- Code adapted from Microsoft Docs: WHILE + RAND + CHOOSE + DATEADD (n.d.-a, n.d.-b, n.d.-c)
SET @i = 1;
WHILE @i <= 200
BEGIN
    INSERT INTO PatientInfo (FirstName, LastName, DOB, Gender, ContactNumber, Address, BloodType, Allergies, MedicalHistory)
    VALUES (
        CONCAT('PatientFirst', @i),
        CONCAT('Last', @i),
        DATEADD(DAY, -FLOOR(RAND() * 15000), GETDATE()),
        CHOOSE((@i % 3) + 1, 'Male', 'Female', 'Other'),
        CONCAT('07', RIGHT('000000000' + CAST(@i AS VARCHAR), 9)),
        CONCAT(@i, ' Lane, Neighborhood'),
        CHOOSE((@i % 4) + 1, 'A+', 'B-', 'O+', 'AB+'),
        CHOOSE((@i % 4) + 1, 'None', 'Peanuts', 'Dust', 'Gluten'),
        CHOOSE((@i % 3) + 1, 'Healthy', 'Diabetes', 'Hypertension')
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Doctors
-- Code adapted from Microsoft Docs WHILE and CHOOSE (n.d.-a, n.d.-b)
SET @i = 1;
WHILE @i <= 20
BEGIN
    INSERT INTO DoctorDirectory (FirstName, LastName, Specialization, ContactNumber, Email, HospitalID, DepartmentID)
    VALUES (
        CONCAT('DocFirst', @i),
        CONCAT('DocLast', @i),
        CHOOSE((@i % 5)+1, 'Cardiology', 'Neurology', 'General', 'Pediatrics', 'Orthopedics'),
        CONCAT('0180000000', @i),
        CONCAT('doctor', @i, '@healthcare.com'),
        ((@i - 1) % 5) + 1,  -- HospitalID
        ((@i - 1) % 5) + 1   -- DepartmentID
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Prescriptions
-- Code adapted from Microsoft Docs: WHILE loop (n.d.-a)
SET @i = 1;
WHILE @i <= 200
BEGIN
    INSERT INTO Prescription (PatientID, Medication, Dosage, Duration)
    VALUES (
        @i,
        CONCAT('Medicine_', @i),
        CONCAT((@i % 3) + 1, ' tablets daily'),
        CONCAT((@i % 7) + 1, ' days')
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Lab Results
-- Code adapted from Microsoft Docs (n.d.-a)
SET @i = 1;
WHILE @i <= 200
BEGIN
    INSERT INTO LabResults (PatientID, TestName, Result, ResultDate)
    VALUES (
        @i,
        CHOOSE((@i % 4) + 1, 'Blood Test', 'MRI', 'X-Ray', 'ECG'),
        CHOOSE((@i % 3) + 1, 'Normal', 'Elevated', 'Low'),
        DATEADD(DAY, -@i, GETDATE())
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Room Assignments
-- Code adapted from Microsoft Docs (n.d.-a)
SET @i = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO RoomAssignments (PatientID, RoomNumber, AdmissionDate, DischargeDate)
    VALUES (
        @i,
        CONCAT('R', @i),
        DATEADD(DAY, -@i, GETDATE()),
        DATEADD(DAY, -@i + 5, GETDATE())
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Billing Records
-- Code adapted from Microsoft Docs (n.d.-a)
SET @i = 1;
WHILE @i <= 150
BEGIN
    INSERT INTO BillingRecords (PatientID, TotalAmount, PaymentStatus)
    VALUES (
        @i,
        ROUND(1000 + (RAND() * 4000), 2),
        CHOOSE((@i % 3) + 1, 'Paid', 'Pending', 'Cancelled')
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Appointments
-- Code adapted from Microsoft Docs (n.d.-a)
SET @i = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO AppointmentSchedule (PatientID, DoctorID, AppointmentDateTime, Status, Notes)
    VALUES (
        @i,
        ((@i - 1) % 20) + 1,
        DATEADD(DAY, @i % 30, GETDATE()),
        CHOOSE((@i % 3) + 1, 'Scheduled', 'Completed', 'Cancelled'),
        CONCAT('Follow-up visit for patient #', @i)
    );
    SET @i += 1;
END;
-- End of adapted code

-- Insert Pharmacy 
-- Code adapted from Microsoft Docs (n.d.-a)
SET @i = 1;
WHILE @i <= 50
BEGIN
    INSERT INTO Pharmacy (DrugName, Manufacturer, ExpiryDate, Stock)
    VALUES (
        CONCAT('Drug_', @i),
        CONCAT('PharmaCorp_', @i),
        DATEADD(DAY, @i * 30, GETDATE()),
        ((@i * 2) % 100)
    );
    SET @i += 1;
END;
-- End of adapted code



-- ==========================INDEX CREATION==================================
-- Improves lookup performance for filtering appointments by doctor
CREATE INDEX idx_Appointment_DoctorID ON AppointmentSchedule(DoctorID);

-- Speeds up queries filtering appointments by patient
CREATE INDEX idx_Appointment_PatientID ON AppointmentSchedule(PatientID);

-- Optimizes filtering appointments by status (Scheduled, Completed, Cancelled)
CREATE INDEX idx_Appointment_Status ON AppointmentSchedule(Status);

-- Enhances performance of queries retrieving billing info by patient
CREATE INDEX idx_Billing_PatientID ON BillingRecords(PatientID);

-- Improves efficiency when filtering billing records by payment status
CREATE INDEX idx_Billing_Status ON BillingRecords(PaymentStatus);

-- Boosts search speed for drug names in pharmacy inventory
CREATE INDEX idx_DrugName ON Pharmacy(DrugName);

-- Speeds up detection of low stock items in pharmacy inventory
CREATE INDEX idx_Drug_Stock ON Pharmacy(Stock);

-- Enhances filtering or grouping of doctors by specialization
CREATE INDEX idx_Doctor_Specialization ON DoctorDirectory(Specialization);

-- Improves querying patient data based on gender
CREATE INDEX idx_Patient_Gender ON PatientInfo(Gender);

-- Boosts performance when searching/filtering by blood type
CREATE INDEX idx_Patient_BloodType ON PatientInfo(BloodType);

-- Speeds up retrieval of lab results for specific patients
CREATE INDEX idx_Lab_PatientID ON LabResults(PatientID);

-- Enhances sorting/filtering lab results by date
CREATE INDEX idx_Lab_ResultDate ON LabResults(ResultDate);

-- Optimizes department searches by department name
CREATE INDEX idx_Department_Name ON Departments(DepartmentName);

-- Improves performance for room assignments filtered by patient
CREATE INDEX idx_Room_PatientID ON RoomAssignments(PatientID);

-- Speeds up queries filtering doctors by associated hospital
CREATE INDEX idx_Doctor_HospitalID ON DoctorDirectory(HospitalID);
GO


-- ==========================CONFIRMATION======================================
PRINT 'All tables and data successfully created!';
GO

-- ==========================VIEW DATA============================
SELECT * FROM PatientInfo;
SELECT * FROM DoctorDirectory;
SELECT * FROM AppointmentSchedule;
SELECT * FROM BillingRecords;
SELECT * FROM Pharmacy;
SELECT * FROM LabResults;
SELECT * FROM RoomAssignments;
SELECT * FROM Departments;
SELECT * FROM Hospital;
SELECT * FROM Prescription;
GO

-- ==========================QUERIES============================
-- List all upcoming appointments for a specific doctor
SELECT a.AppointmentID, p.FirstName AS Patient, p.LastName, a.AppointmentDateTime, a.Status
FROM AppointmentSchedule a
JOIN PatientInfo p ON a.PatientID = p.PatientID
WHERE a.DoctorID = 1 AND a.Status = 'Scheduled'
ORDER BY a.AppointmentDateTime;
GO

-- Patients with 'Asthma' in their medical history
SELECT PatientID, FirstName, LastName, MedicalHistory 
FROM PatientInfo
WHERE MedicalHistory LIKE '%Asthma%';
GO

-- Total revenue from paid bills
SELECT SUM(TotalAmount) AS TotalRevenue 
FROM BillingRecords
WHERE PaymentStatus = 'Paid';
GO

-- Drugs low in stock
SELECT DrugID, DrugName, Stock
FROM Pharmacy
WHERE Stock < 20;
GO

-- Count doctors by specialization
SELECT Specialization, COUNT(*) AS TotalDoctors
FROM DoctorDirectory
GROUP BY Specialization;
GO

-- Count total number of patients by gender
SELECT Gender, COUNT(*) AS TotalPatients
FROM PatientInfo
GROUP BY Gender;
GO

-- Recent Lab Results for a specific patient
SELECT lr.TestName, lr.Result, lr.ResultDate, CONCAT(p.FirstName, ' ', p.LastName) AS Patient
FROM LabResults lr
JOIN PatientInfo p ON lr.PatientID = p.PatientID
WHERE p.PatientID = 2
ORDER BY lr.ResultDate DESC;
GO

-- Upcoming appointments (next 7 days)
SELECT a.AppointmentID, CONCAT(p.FirstName, ' ', p.LastName) AS Patient,
       CONCAT(d.FirstName, ' ', d.LastName) AS Doctor,
       a.AppointmentDateTime
FROM AppointmentSchedule a
JOIN PatientInfo p ON a.PatientID = p.PatientID
JOIN DoctorDirectory d ON a.DoctorID = d.DoctorID
WHERE a.AppointmentDateTime BETWEEN GETDATE() AND DATEADD(day, 7, GETDATE())
ORDER BY a.AppointmentDateTime;
GO

-- ==========================REFERENCES============================
-- Microsoft Docs. (n.d.-a). WHILE (Transact-SQL). Retrieved March 29, 2025, from https://learn.microsoft.com/en-us/sql/t-sql/language-elements/while-transact-sql
-- Microsoft Docs. (n.d.-b). CHOOSE (Transact-SQL). Retrieved March 29, 2025, from https://learn.microsoft.com/en-us/sql/t-sql/functions/choose-transact-sql
-- Microsoft Docs. (n.d.-c). CONCAT (Transact-SQL). Retrieved March 29, 2025, from https://learn.microsoft.com/en-us/sql/t-sql/functions/concat-transact-sql
-- Microsoft Docs. (n.d.-d). DROP DATABASE (Transact-SQL). Retrieved March 29, 2025, from https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-database-transact-sql?view=sql-server-ver16
