pipeline {
    agent any

    triggers {
        // 11:00 AM Bangkok (UTC+7) = 04:00 UTC every day
        cron('0 4 * * *')
    }

    environment {
        PYTHON = 'python3'
        VENV   = "${WORKSPACE}/.venv"
    }

    stages {
        stage('Setup') {
            steps {
                sh '''
                    ${PYTHON} -m venv ${VENV}
                    ${VENV}/bin/pip install --upgrade pip -q
                    ${VENV}/bin/pip install -r requirements.txt -q
                '''
            }
        }

        stage('Run Pipeline') {
            steps {
                withCredentials([
                    string(credentialsId: 'MONGO_URI', variable: 'MONGO_URI'),
                    file(credentialsId: 'GOOGLE_SERVICE_ACCOUNT_JSON', variable: 'SA_JSON_PATH')
                ]) {
                    sh '''
                        export GOOGLE_CREDENTIALS_PATH=${SA_JSON_PATH}
                        ${VENV}/bin/python main.py
                    '''
                }
            }
        }
    }

    post {
        failure {
            echo "Pipeline failed — check logs above"
        }
        success {
            echo "Pipeline completed successfully"
        }
    }
}
