#!/usr/bin/env python3
"""
Script de validación rápida de conexión a Clockify API.
Ejecutar: python test_connection.py
"""
import os
from dotenv import load_dotenv

# Intentar cargar .env si existe
load_dotenv()

def test_clockify_connection():
    """Prueba la conexión a Clockify API"""
    import requests

    api_key = os.getenv("CLOCKIFY_API_KEY")
    workspace_id = os.getenv("CLOCKIFY_WORKSPACE_ID")

    print("🔍 Validando configuración de Clockify...")
    print("-" * 50)

    # Validar que existen las variables
    if not api_key:
        print("❌ ERROR: CLOCKIFY_API_KEY no está configurada")
        print("   Configúrala en .env o como variable de entorno")
        return False

    if not workspace_id:
        print("❌ ERROR: CLOCKIFY_WORKSPACE_ID no está configurada")
        print("   Configúrala en .env o como variable de entorno")
        return False

    print(f"✅ CLOCKIFY_API_KEY: {'*' * 20}{api_key[-4:]}")
    print(f"✅ CLOCKIFY_WORKSPACE_ID: {workspace_id}")
    print()

    # Test 1: Get user info
    print("📡 Test 1: Obteniendo información del usuario...")
    try:
        response = requests.get(
            "https://api.clockify.me/api/v1/user",
            headers={"X-Api-Key": api_key},
            timeout=10
        )

        if response.status_code == 200:
            user = response.json()
            print(f"✅ Conectado como: {user.get('name')} ({user.get('email')})")
        elif response.status_code == 401:
            print("❌ ERROR: API Key inválida")
            print("   Genera una nueva en: https://app.clockify.me/user/settings")
            return False
        else:
            print(f"❌ ERROR: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ ERROR de conexión: {e}")
        return False

    print()

    # Test 2: Get workspace info
    print("📡 Test 2: Validando acceso al workspace...")
    try:
        response = requests.get(
            f"https://api.clockify.me/api/v1/workspaces/{workspace_id}",
            headers={"X-Api-Key": api_key},
            timeout=10
        )

        if response.status_code == 200:
            workspace = response.json()
            print(f"✅ Workspace: {workspace.get('name')}")
        elif response.status_code == 403:
            print("❌ ERROR: No tienes acceso a este workspace")
            return False
        elif response.status_code == 404:
            print("❌ ERROR: Workspace no encontrado")
            print("   Verifica el WORKSPACE_ID")
            return False
        else:
            print(f"❌ ERROR: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ ERROR de conexión: {e}")
        return False

    print()

    # Test 3: Get sample data
    print("📡 Test 3: Obteniendo datos de ejemplo...")
    try:
        # Users
        response = requests.get(
            f"https://api.clockify.me/api/v1/workspaces/{workspace_id}/users",
            headers={"X-Api-Key": api_key},
            params={"page-size": 5},
            timeout=10
        )
        users_count = len(response.json()) if response.status_code == 200 else 0
        print(f"   👥 Users: {users_count} encontrados")

        # Projects
        response = requests.get(
            f"https://api.clockify.me/api/v1/workspaces/{workspace_id}/projects",
            headers={"X-Api-Key": api_key},
            params={"page-size": 5},
            timeout=10
        )
        projects_count = len(response.json()) if response.status_code == 200 else 0
        print(f"   📊 Projects: {projects_count} encontrados")

        # Clients
        response = requests.get(
            f"https://api.clockify.me/api/v1/workspaces/{workspace_id}/clients",
            headers={"X-Api-Key": api_key},
            params={"page-size": 5},
            timeout=10
        )
        clients_count = len(response.json()) if response.status_code == 200 else 0
        print(f"   🏢 Clients: {clients_count} encontrados")

        if users_count > 0 or projects_count > 0 or clients_count > 0:
            print("✅ Datos disponibles para sincronizar")
        else:
            print("⚠️  ADVERTENCIA: No se encontraron datos")
            print("   El workspace puede estar vacío")

    except Exception as e:
        print(f"⚠️  ERROR al obtener datos: {e}")

    print()
    print("=" * 50)
    print("✅ VALIDACIÓN COMPLETADA - Conexión exitosa!")
    print("=" * 50)
    print()
    print("Siguiente paso: Ejecutar sync completo")
    print("  python main.py")

    return True


def test_bigquery_connection():
    """Prueba la conexión a BigQuery"""
    from google.cloud import bigquery

    project = os.getenv("BQ_PROJECT")

    print()
    print("🔍 Validando configuración de BigQuery...")
    print("-" * 50)

    if not project:
        print("❌ ERROR: BQ_PROJECT no está configurada")
        return False

    print(f"✅ BQ_PROJECT: {project}")

    try:
        client = bigquery.Client(project=project)
        # Intentar listar datasets
        datasets = list(client.list_datasets(max_results=1))
        print(f"✅ Conexión exitosa a BigQuery")
        return True
    except Exception as e:
        print(f"❌ ERROR de conexión a BigQuery: {e}")
        print("   Verifica que tengas credenciales de GCP configuradas")
        print("   (Application Default Credentials o GOOGLE_APPLICATION_CREDENTIALS)")
        return False


if __name__ == "__main__":
    print()
    print("=" * 50)
    print("  VALIDACIÓN DE CONEXIÓN - Clockify to BigQuery")
    print("=" * 50)
    print()

    clockify_ok = test_clockify_connection()

    if clockify_ok:
        bigquery_ok = test_bigquery_connection()

        if bigquery_ok:
            print()
            print("🎉 TODO LISTO - Puedes ejecutar el sync completo!")
        else:
            print()
            print("⚠️  BigQuery no está configurado correctamente")
            print("   El sync fallará al intentar cargar datos")
    else:
        print()
        print("❌ FALLO - Corrige los errores de Clockify antes de continuar")

    print()
