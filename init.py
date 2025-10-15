from sqlalchemy import text

def init_db(app, db, bcrypt):
    with app.app_context():
        create_users_table = """
        CREATE TABLE IF NOT EXISTS users(
            id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            password VARCHAR(255),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """

        create_file_uploads_table = """
        CREATE TABLE IF NOT EXISTS file_uploads(
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            filename VARCHAR(255) NOT NULL,
            filetype VARCHAR(100) NOT NULL,
            file_size VARCHAR(50) NOT NULL,
            date_created DATE NOT NULL,
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id),
            INDEX idx_upload_date (upload_date)
        );
        """

        # Wrap SQL in text()
        db.session.execute(text(create_users_table))
        db.session.execute(text(create_file_uploads_table))
        db.session.commit()

        # Check and insert admin user
        admin_email = "admin@gmail.com"
        admin_user = db.session.execute(
            text("SELECT * FROM users WHERE email = :email"),
            {"email": admin_email}
        ).fetchone()

        if not admin_user:
            hashed_pw = bcrypt.generate_password_hash("Admin123@").decode('utf-8')
            db.session.execute(
                text(
                    "INSERT INTO users (email, firstname, lastname, password) "
                    "VALUES (:email, :firstname, :lastname, :password)"
                ),
                {
                    "email": admin_email,
                    "firstname": "admin",
                    "lastname": "admin",
                    "password": hashed_pw
                }
            )
            db.session.commit()
            print("\n ------ Admin user created -----------")
        else:
            print("\n ------ Admin user already exists -----------")
        print("Database initialization completed.\n")