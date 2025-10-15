from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from db import get_db_uri
from init import init_db
from flask_cors import CORS
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from flask import send_from_directory
from werkzeug.security import safe_join

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'data/inbox')
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    firstname = db.Column(db.String(255), nullable=True)
    lastname = db.Column(db.String(255), nullable=True)
    password = db.Column(db.String(255), nullable=False)
    
class FileUploads(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    filetype = db.Column(db.String(100), nullable=False)
    file_size = db.Column(db.String(50), nullable=False)
    date_created = db.Column(db.Date, nullable=False)
    upload_date = db.Column(db.DateTime, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, server_default=db.func.current_timestamp(), server_onupdate=db.func.current_timestamp())

# initializer la base de données
with app.app_context():
    init_db(app, db, bcrypt)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# point d'entrée pour cree un utilisateur
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    if Users.query.filter_by(email=email).first():
        return jsonify({"error": "Email deja enregistre"}), 400

    hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
    new_user = Users(email=email, password=hashed_pw, firstname=data.get('firstname'), lastname=data.get('lastname'))
    db.session.add(new_user)
    db.session.commit()

    return jsonify({"message": "Utilisateur cree avec succes"}), 201

# point d'entrée pour connecter un utilisateur
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = Users.query.filter_by(email=email).first()
    print("User found: " + str(user))
    if user and bcrypt.check_password_hash(user.password, password):
        return jsonify({"message": "Connexion reussie", "data": {"email": user.email, "id": user.id}}), 200
    else:
        return jsonify({"error": "Email ou mot de passe invalide"}), 401
    
@app.route('/data/inbox/<path:filename>')
def serve_file(filename):
    safe_path = safe_join(UPLOAD_FOLDER, filename)
    if not safe_path or not os.path.exists(safe_path):
        return jsonify({"error": "Fichier non trouvé"}), 404
    return send_from_directory(UPLOAD_FOLDER, filename)
    
@app.route('/files', methods=['GET'])
def list_files():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({"error": "L'utilisateur n'est pas connecté. Veuillez vous connecter puis réessayer."}), 400

    files = FileUploads.query.filter_by(user_id=user_id).all()
    files_data = [
        {
            "id": f.id,
            "filename": f.filename,
            "filetype": f.filetype,
            "file_size": f.file_size,
            "date_created": f.date_created.strftime('%Y-%m-%d'),
            "upload_date": f.upload_date.strftime('%Y-%m-%d %H:%M:%S'),
            "updated_at": f.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        } for f in files
    ]
    return jsonify({"files": files_data}), 200
    
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "Aucun fichier dans la requête"}), 400
    
    file = request.files['file']
    user_id = request.args.get('user_id')
    create_date = request.args.get('create_date')

    if file.filename == '':
        return jsonify({"error": "Nom de fichier vide"}), 400

    if file and allowed_file(file.filename):
        original_filename = secure_filename(file.filename)
        file_ext = os.path.splitext(original_filename)[1]

        # pour nommer le fichier: yyyy_mm_dd
        # datetime.now().strftime("%Y_%m_%d")
        new_filename = datetime.now().strftime("%Y_%m_%d")+file_ext
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)

        file.save(filepath)
        stat_info = os.stat(filepath)
        
        created_at = datetime.strptime(create_date, '%Y-%m-%d')
        size_mb = stat_info.st_size / (1024*1024)
        file_size_str = f"{size_mb:.4f} MB"

        new_upload = FileUploads(
            user_id=user_id,
            filename=new_filename,
            filetype=file.content_type,
            file_size=file_size_str,
            date_created=created_at
        )
        db.session.add(new_upload)
        db.session.commit()

        return jsonify({
            "message": "Fichier uploadé avec succès",
            "filename": new_filename,
            "path": filepath
        }), 201
    else:
        return jsonify({"error": "Une erreur s'est produite. Veuillez vérifier le type/taille (csv uniquement) du fichier puis réessayer."}), 400


@app.route('/delete-file/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    user_id = request.args.get('user_id', type=int)

    if not user_id:
        return jsonify({"error": "ID utilisateur requis"}), 400

    file_record = FileUploads.query.filter_by(id=file_id).first()

    if not file_record:
        return jsonify({"error": "Fichier introuvable"}), 404

    if file_record.user_id != user_id:
        return jsonify({"error": "Vous n'êtes pas autorisé à supprimer ce fichier"}), 403

    filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file_record.filename))
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
        except Exception as e:
            return jsonify({"error": f"Impossible de supprimer le fichier physique: {str(e)}"}), 500
    else:
        return jsonify({"warning": "Fichier physique déjà supprimé"}), 200

    try:
        db.session.delete(file_record)
        db.session.commit()
    except Exception as e:
        return jsonify({"error": f"Impossible de supprimer l'enregistrement en base: {str(e)}"}), 500

    return jsonify({"message": "Fichier supprimé avec succès"}), 200

if __name__ == '__main__':
    app.run(debug=True)

