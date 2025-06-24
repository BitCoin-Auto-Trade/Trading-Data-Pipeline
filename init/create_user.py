from airflow import settings
from airflow.auth.managers.fab.models import User, Role
from werkzeug.security import generate_password_hash

session = settings.Session()
admin_role = session.query(Role).filter_by(name="Admin").first()
if not admin_role:
    admin_role = Role(name="Admin")
    session.add(admin_role)
    session.commit()

if not session.query(User).filter_by(username="admin").first():
    user = User(
        username="admin",
        email="admin@example.com",
        is_active=True,
        password=generate_password_hash("admin"),
        roles=[admin_role],
    )
    session.add(user)
    session.commit()
    print("✅ admin 사용자 생성 완료")
else:
    print("⚠️ admin 사용자가 이미 존재합니다.")
