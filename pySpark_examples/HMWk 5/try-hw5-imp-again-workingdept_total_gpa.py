import sys

from pyspark import SparkConf, SparkContext


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

    # Calculate average GPA per department
    department_total_gpa = department_gpa.map(lambda x: (x[1][1], (x[1][0], 1)))
    department_avg_gpa = department_total_gpa.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / x[1])

    # Sort and collect the results
    sorted_results = department_avg_gpa.sortByKey().collect()

    department_avg_gpa.saveAsTextFile(sys.argv[1])

    sc.stop()


if __name__ == "__main__":
    main()
