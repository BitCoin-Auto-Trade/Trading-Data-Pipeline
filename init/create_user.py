from airflow import settings
from flask_appbuilder.security.sqla.models import User, Role
from werkzeug.security import generate_password_hash

def create_admin_user():
    session = settings.Session()

    # Admin 역할 가져오기
    admin_role = session.query(Role).filter_by(name="Admin").first()
    if not admin_role:
        admin_role = Role(name="Admin")
        session.add(admin_role)
        session.commit()

    # 사용자 존재 확인
    if not session.query(User).filter_by(username="admin").first():
        user = User(
            username="admin",
            first_name="admin",
            last_name="admin",
            email="admin@example.com",
            active=True,
            password=generate_password_hash("admin"),
            roles=[admin_role]
        )
        session.add(user)
        session.commit()
        print("✅ admin 사용자 생성 완료")
    else:
        print("⚠️ 이미 admin 사용자가 존재합니다.")

if __name__ == "__main__":
    create_admin_user()
