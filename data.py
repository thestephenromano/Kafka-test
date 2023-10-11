import random


# Helper function to generate random DOB
def generate_dob():
    year = random.randint(1940, 2005)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Simplification: up to 28 to avoid month complexities
    return f"{year}-{month:02}-{day:02}"


# Sample data for names, diagnoses, and medications
first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
last_names = ["Doe", "Smith", "Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"]
diagnoses = ["Hypertension", "Type 2 Diabetes", "Asthma", "Depression", "Osteoarthritis", "Heart Disease", "Migraine",
             "Allergies", "Eczema", "GERD"]
medications_list = [
    ['Lisinopril', 'Amlodipine'],
    ['Metformin', 'Januvia'],
    ['Ventolin', 'Singulair'],
    ['Prozac', 'Wellbutrin'],
    ['Ibuprofen', 'Paracetamol'],
    ['Aspirin', 'Plavix'],
    ['Sumatriptan', 'Rizatriptan'],
    ['Zyrtec', 'Claritin'],
    ['Clobetasol', 'Hydrocortisone'],
    ['Omeprazole', 'Lansoprazole']
]


def generate_patients(num):
    for i in range(num):
        patient = {
            'patient_id': f"{i + 1:03}",
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'date_of_birth': generate_dob(),
            'diagnosis': random.choice(diagnoses),
            'medications': random.choice(medications_list),
            'last_visit': f"2023-{random.randint(1, 10):02}-{random.randint(1, 28):02}"
        }
        yield patient
