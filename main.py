from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03(session):
    """
    SELECT g.name AS group_name, AVG(gr.grade) AS average_grade
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    JOIN groups g ON s.group_id = g.id
    JOIN subjects sub ON gr.subject_id = sub.id
    WHERE sub.name = 'pick' -- Замініть на актуальну назву предмета
    GROUP BY g.name
    ORDER BY average_grade DESC;
    """
    result = session.query(Group.name.label('group_name'), func.avg(Grade.grade).label('average_grade')) \
        .join(Student, Group.id == Student.group_id) \
        .join(Grade, Student.id == Grade.student_id) \
        .join(Subject, Grade.subject_id == Subject.id) \
        .filter(Subject.name == 'pick').group_by(Group.name).order_by(desc('average_grade')).all()
    return result


def select_04(session):
    """SELECT AVG(grade) AS average_grade FROM grades;"""
    result = session.query(func.avg(Grade.grade).label('average_grade')).scalar()
    return result


def select_05(session):
    """SELECT sub.name 
    FROM subjects sub
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.fullname = 'Joshua Martin';  -- Замініть на актуальне ім'я"""
    result = session.query(Subject.name).join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Teacher.fullname == 'Joshua Martin').all()
    return result


def select_06(session):
    """SELECT s.fullname
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.name = 'move';  -- Замініть на актуальну назву групи"""
    result = session.query(Student.fullname).join(Group, Student.group_id == Group.id) \
        .filter(Group.name == 'move').all()
    return result


def select_07(session):
    """SELECT s.fullname, g.grade FROM grades g
    JOIN students s ON g.student_id = s.id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE s.group_id = (SELECT id FROM groups WHERE name = 'language')
    AND sub.name = 'sing';"""
    subquery = session.query(Group.id).filter(Group.name == 'language').subquery()
    result = session.query(Student.fullname, Grade.grade) \
        .join(Subject, Grade.subject_id == Subject.id) \
        .filter(and_(Student.group_id == subquery, Subject.name == 'sing')).all()
    return result


def select_08(session):
    """SELECT AVG(g.grade) AS average_grade FROM grades g
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE sub.teacher_id = (SELECT id FROM teachers WHERE fullname = 'Anna Calderon');"""
    subquery = session.query(Teacher.id).filter(Teacher.fullname == 'Anna Calderon').subquery()
    result = session.query(func.avg(Grade.grade).label('average_grade')) \
        .join(Subject, Grade.subject_id == Subject.id) \
        .filter(Subject.teacher_id == subquery).scalar()
    return result


def select_09(session):
    """SELECT DISTINCT sub.name FROM grades g
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE g.student_id = (SELECT id FROM students WHERE fullname = 'Bonnie Wolfe');"""
    subquery = session.query(Student.id).filter(Student.fullname == 'Bonnie Wolfe').subquery()
    result = session.query(Subject.name).join(Grade, Subject.id == Grade.subject_id) \
        .filter(Grade.student_id == subquery).distinct().all()
    return result


def select_10(session):
    """SELECT DISTINCT sub.name FROM grades g
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN students s ON g.student_id = s.id
    WHERE s.fullname = 'William Lopez'
    AND sub.teacher_id = (SELECT id FROM teachers WHERE fullname = 'George Cuevas');"""
    subquery = session.query(Teacher.id).filter(Teacher.fullname == 'George Cuevas').subquery()
    result = session.query(Subject.name).join(Grade, Subject.id == Grade.subject_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(and_(Student.fullname == 'William Lopez', Subject.teacher_id == subquery)).distinct().all()
    return result



def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_12())
