import sys

from pyspark import SparkConf, SparkContext, RDD


def parse_student(line):
    parts = line.split(" ")
    student_id = parts[0]
    dept_id = parts[-1]
    return (student_id, dept_id)


def parse_course(line):
    parts = line.split(" ")
    student_id = parts[0]
    grade = float(parts[2])
    return (student_id, grade)


def parse_department(line):
    parts = line.split(" ")
    dept_id = parts[0]
    dept_name = parts[1]
    return (dept_id, dept_name)

def create_dept_gpa_pair(student_course_tuple):
    dept_id = student_course_tuple[1][0]  # Extracting department ID
    gpa = student_course_tuple[1][1]      # Extracting GPA
    return (dept_id, gpa)

def join_with_department_data(rdd, department_pairs):
    return rdd.join(department_pairs)

def map_dept_gpa(dept_gpa_tuple):
    dept_name = dept_gpa_tuple[1][1]  # Extracting department name
    gpa = dept_gpa_tuple[1][0]        # Extracting GPA
    return (dept_name, (gpa, 1))


def reduce_gpa_total_and_count(a, b):
    total_gpa_a, count_a = a
    total_gpa_b, count_b = b
    result = ((total_gpa_a + total_gpa_b), (count_a + count_b))

    # Print the result before returning
    print(f"Combining {a} and {b} to get {result}")

    return result


def calculate_avg_gpa(dept_gpa_total):
    total_gpa, count = dept_gpa_total
    return total_gpa / count

def format_result(tup):
    department, (total_gpa, count) = tup
    str= f"\n\n\n\nDepartment: {department}, Total GPA: {total_gpa}, Count: {count}"
    print(str)
    return str

def main():
    conf = SparkConf().setAppName("Calculate Average GPA").setMaster("local")
    sc = SparkContext(conf=conf)

    # Load datasets
    students = sc.textFile("C:/Users/selaz/Downloads/SPARK/StudentAnalysis/input/student.txt")
    courses = sc.textFile("C:/Users/selaz/Downloads/SPARK/StudentAnalysis/input/Course.txt")
    departments = sc.textFile("C:/Users/selaz/Downloads/SPARK/StudentAnalysis/input/Dept.txt")

    # Create PairRDDs
    student_pairs = students.map(parse_student)
    course_pairs = courses.map(parse_course)
    department_pairs = departments.map(parse_department)

    # Join operations
    student_course = student_pairs.join(course_pairs) #('374899', ('101', 4.0))
    dept_gpa_pairs = student_course.map(create_dept_gpa_pair) #('101', (4.0, 'HumanResources'))

    # Perform the join operation with department data
    department_gpa = join_with_department_data(dept_gpa_pairs, department_pairs)
####
    # Calculate average GPA per department
    department_total_gpa = department_gpa.map(map_dept_gpa)

    # Reduce by key to calculate total GPA and count for each department
    department_total_gpa_and_count = department_total_gpa.reduceByKey(reduce_gpa_total_and_count)

    print("\n\n\n\nType of department_total_gpa:", type(department_total_gpa))
    print("Type of department_total_gpa_and_count:", type(department_total_gpa_and_count))

    formatted_rdd = department_total_gpa_and_count.map(format_result)


    formatted_rdd.saveAsTextFile(sys.argv[1])

    # Calculate the average GPA for each department
    department_avg_gpa = department_total_gpa_and_count.mapValues(calculate_avg_gpa)

    # Sort and collect the results
    sorted_results = department_avg_gpa.sortByKey().collect()



    sc.stop()


if __name__ == "__main__":
    main()
