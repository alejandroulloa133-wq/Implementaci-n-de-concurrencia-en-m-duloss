#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <random>
#include <vector>

// 1. Implementación de un Semáforo clásico utilizando mecanismos de C++11
class Semaphore {
private:
    std::mutex mtx;
    std::condition_variable cv;
    int count;

public:
    Semaphore(int initial_count = 0) : count(initial_count) {}

    // Operación WAIT (P) - Disminuye el contador o bloquea si es 0
    void wait() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]() { return count > 0; });
        count--;
    }

    // Operación SIGNAL (V) - Incrementa el contador y notifica a los hilos
    void signal() {
        std::unique_lock<std::mutex> lock(mtx);
        count++;
        cv.notify_one();
    }
};

// 2. Variables Globales y Recursos Compartidos
const int BUFFER_CAPACITY = 5;
std::queue<int> traffic_buffer;

// Semáforos y Mutex para la sincronización
Semaphore empty_slots(BUFFER_CAPACITY); // Cuenta espacios libres en el búfer
Semaphore full_slots(0);                // Cuenta datos disponibles en el búfer
std::mutex buffer_mutex;                // Exclusión mutua para modificar la cola (búfer)

bool simulation_running = true;

// 3. Proceso Productor: Sensores de Tráfico
void traffic_sensor(int sensor_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(5, 50); // Genera entre 5 y 50 vehículos
    std::uniform_int_distribution<> delay(300, 800); // Retardo aleatorio

    for (int i = 0; i < 4; ++i) { // Cada sensor envía 4 lotes de datos
        int cars_detected = distrib(gen);

        // Protocolo de entrada
        empty_slots.wait();         // Espera si el búfer está lleno
        buffer_mutex.lock();        // Bloquea el acceso concurrente al búfer

        // Sección Crítica
        traffic_buffer.push(cars_detected);
        std::cout << "[+] Sensor " << sensor_id << " detecto " << cars_detected 
                  << " vehiculos. (Búfer: " << traffic_buffer.size() << "/" << BUFFER_CAPACITY << ")\n";

        // Protocolo de salida
        buffer_mutex.unlock();      // Libera el búfer
        full_slots.signal();        // Avisa que hay un nuevo dato disponible

        std::this_thread::sleep_for(std::chrono::milliseconds(delay(gen)));
    }
}

// 4. Proceso Consumidor: Módulos de Análisis
void analysis_module(int module_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> delay(500, 1000);

    while (simulation_running || !traffic_buffer.empty()) {
        // Protocolo de entrada (con timeout para no quedar bloqueado al final)
        full_slots.wait(); 
        
        buffer_mutex.lock();
        if (traffic_buffer.empty()) {
            buffer_mutex.unlock();
            break; // Salida de seguridad
        }

        // Sección Crítica
        int data_to_process = traffic_buffer.front();
        traffic_buffer.pop();
        std::cout << "[-] Modulo Analista " << module_id << " procesando " 
                  << data_to_process << " vehiculos. (Quedan: " << traffic_buffer.size() << ")\n";

        // Protocolo de salida
        buffer_mutex.unlock();
        empty_slots.signal(); // Avisa que se liberó un espacio en el búfer

        std::this_thread::sleep_for(std::chrono::milliseconds(delay(gen)));
    }
}

int main() {
    std::cout << "=== INICIANDO SIMULACION SIGET (Productor-Consumidor) ===\n\n";

    std::vector<std::thread> sensors;
    std::vector<std::thread> analyzers;

    // Crear 3 procesos productores (Sensores)
    for (int i = 1; i <= 3; ++i) {
        sensors.push_back(std::thread(traffic_sensor, i));
    }

    // Crear 2 procesos consumidores (Módulos de Análisis)
    for (int i = 1; i <= 2; ++i) {
        analyzers.push_back(std::thread(analysis_module, i));
    }

    // Esperar a que los sensores terminen de transmitir
    for (auto& s : sensors) {
        s.join();
    }

    // Indicar a los analistas que la simulación terminó
    simulation_running = false;
    
    // Inyectar señales falsas para destrabar a los analistas en espera
    full_slots.signal(); 
    full_slots.signal();

    for (auto& a : analyzers) {
        a.join();
    }

    std::cout << "\n=== SIMULACION FINALIZADA CON EXITO ===\n";
    std::cout << "Todos los datos fueron procesados sin corrupciom ni bloqueos.\n";

    return 0;
}
