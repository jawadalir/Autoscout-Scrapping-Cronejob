import requests
import time
import os
from datetime import datetime
import sys
import json
from colorama import init, Fore, Style, Back

# Initialize colorama for Windows
init(autoreset=True)

class WindowsScraperMonitor:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        
    def clear_screen(self):
        """Clear console screen (Windows compatible)."""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def check_health(self):
        """Check if API is running."""
        try:
            response = requests.get(f"{self.base_url}/api/health", timeout=5)
            return response.status_code == 200, response.json()
        except:
            return False, {}
    
    def get_status(self):
        """Get scraper status."""
        try:
            response = requests.get(f"{self.base_url}/api/scrape/status", timeout=5)
            return response.status_code == 200, response.json()
        except:
            return False, {}
    
    def get_last_run(self):
        """Get last run statistics."""
        try:
            response = requests.get(f"{self.base_url}/api/scrape/last-run", timeout=5)
            return response.status_code == 200, response.json()
        except:
            return False, {}
    
    def trigger_scrape(self):
        """Manually trigger scraping."""
        try:
            response = requests.post(f"{self.base_url}/api/scrape/trigger", timeout=5)
            return response.status_code == 200, response.json()
        except:
            return False, {}
    
    def monitor_files(self):
        """Monitor output files."""
        files_to_check = [
            "abc3.txt",
            "new_links.txt", 
            "latest_links.txt",
            "scraper.log",
            "main_links.txt",
            "cleaned_vehicles_*.csv",
            "complete_vehicles_*.csv"
        ]
        
        file_status = {}
        
        # Check specific files
        for file in ["abc3.txt", "new_links.txt", "latest_links.txt", "scraper.log"]:
            if os.path.exists(file):
                size = os.path.getsize(file)
                modified = datetime.fromtimestamp(os.path.getmtime(file))
                file_status[file] = {
                    "exists": True,
                    "size_kb": size / 1024,
                    "modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
                    "age_hours": (datetime.now() - modified).total_seconds() / 3600
                }
            else:
                file_status[file] = {"exists": False}
        
        # Check for CSV files
        import glob
        csv_files = glob.glob("cleaned_vehicles_*.csv") + glob.glob("complete_vehicles_*.csv")
        for csv_file in csv_files[:3]:  # Show only first 3
            if os.path.exists(csv_file):
                size = os.path.getsize(csv_file)
                modified = datetime.fromtimestamp(os.path.getmtime(csv_file))
                file_status[csv_file] = {
                    "exists": True,
                    "size_kb": size / 1024,
                    "modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
                    "rows": self.count_csv_rows(csv_file)
                }
        
        return file_status
    
    def count_csv_rows(self, filepath):
        """Count rows in CSV file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return sum(1 for line in f) - 1  # Subtract header
        except:
            return 0
    
    def check_mongodb(self):
        """Check MongoDB connection."""
        try:
            from app.database.mongo_handler import MongoDBHandler
            mongo = MongoDBHandler()
            count = mongo.collection.count_documents({})
            mongo.close()
            return True, count
        except Exception as e:
            return False, str(e)
    
    def display_dashboard(self):
        """Display Windows-compatible dashboard."""
        try:
            import msvcrt
            has_msvcrt = True
        except:
            has_msvcrt = False
        
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)
        print("🚗 AutoScout24 Scraper Monitor - Windows Edition")
        print("=" * 70 + Style.RESET_ALL)
        
        while True:
            self.clear_screen()
            
            # Header
            print(Fore.CYAN + Style.BRIGHT + "=" * 70)
            print("🚗 AutoScout24 Scraper Monitor - Windows Edition")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70 + Style.RESET_ALL)
            
            # Health status
            health_ok, health_data = self.check_health()
            print(f"\n📡 {Fore.BLUE}API Status:{Style.RESET_ALL}")
            if health_ok:
                print(f"   {Fore.GREEN}✅ ONLINE{Style.RESET_ALL}")
                if health_data:
                    print(f"   Status: {health_data.get('status', 'N/A')}")
                    print(f"   Service: {health_data.get('service', 'N/A')}")
            else:
                print(f"   {Fore.RED}❌ OFFLINE{Style.RESET_ALL}")
                print(f"   {Fore.YELLOW}Make sure the server is running:{Style.RESET_ALL}")
                print(f"   {Fore.WHITE}python run.py{Style.RESET_ALL}")
            
            # Scheduler status
            status_ok, status_data = self.get_status()
            print(f"\n⏰ {Fore.BLUE}Scheduler Status:{Style.RESET_ALL}")
            if status_ok:
                if status_data.get("scheduler_running"):
                    print(f"   {Fore.GREEN}✅ RUNNING{Style.RESET_ALL}")
                else:
                    print(f"   {Fore.YELLOW}⚠️ STOPPED{Style.RESET_ALL}")
                
                if status_data.get("next_run"):
                    print(f"   Next run: {status_data['next_run']}")
                print(f"   Scheduled jobs: {status_data.get('scheduled_jobs', 0)}")
            else:
                print(f"   {Fore.RED}❌ UNKNOWN{Style.RESET_ALL}")
            
            # Last run info
            last_ok, last_data = self.get_last_run()
            print(f"\n📊 {Fore.BLUE}Last Run:{Style.RESET_ALL}")
            if last_ok and last_data.get("data"):
                run_data = last_data["data"]
                if "start_time" in run_data:
                    start_time = run_data["start_time"]
                    if isinstance(start_time, str):
                        start_time = start_time.replace("T", " ").split(".")[0]
                    print(f"   Started: {start_time}")
                
                if "status" in run_data:
                    status = run_data["status"]
                    color = Fore.GREEN if status == "completed" else Fore.RED
                    print(f"   Status: {color}{status}{Style.RESET_ALL}")
                
                if "duration" in run_data:
                    print(f"   Duration: {run_data['duration']:.2f} seconds")
                
                if "steps" in run_data:
                    print(f"   Steps completed: {len(run_data['steps'])}")
            else:
                print(f"   {Fore.YELLOW}No runs recorded yet{Style.RESET_ALL}")
            
            # File status
            print(f"\n📁 {Fore.BLUE}File Status:{Style.RESET_ALL}")
            file_status = self.monitor_files()
            
            for filename, status in file_status.items():
                if status.get("exists"):
                    if filename.endswith('.csv'):
                        rows = status.get('rows', 0)
                        print(f"   {Fore.GREEN}✅ {filename}{Style.RESET_ALL}")
                        print(f"      Size: {status['size_kb']:.1f} KB, Rows: {rows}")
                        print(f"      Modified: {status['modified']}")
                    else:
                        print(f"   {Fore.GREEN}✅ {filename}{Style.RESET_ALL}")
                        print(f"      Size: {status['size_kb']:.1f} KB")
                        print(f"      Modified: {status['modified']}")
                        print(f"      Age: {status['age_hours']:.1f} hours")
                else:
                    if filename in ["abc3.txt", "new_links.txt", "scraper.log"]:
                        print(f"   {Fore.YELLOW}⚠️ {filename}: NOT FOUND{Style.RESET_ALL}")
            
            # MongoDB status
            print(f"\n🗄️ {Fore.BLUE}MongoDB Status:{Style.RESET_ALL}")
            try:
                mongo_ok, mongo_result = self.check_mongodb()
                if mongo_ok:
                    if isinstance(mongo_result, int):
                        print(f"   {Fore.GREEN}✅ CONNECTED{Style.RESET_ALL}")
                        print(f"   Records in database: {mongo_result}")
                    else:
                        print(f"   {Fore.GREEN}✅ CONNECTED{Style.RESET_ALL}")
                else:
                    print(f"   {Fore.RED}❌ CONNECTION FAILED{Style.RESET_ALL}")
                    print(f"   Error: {mongo_result[:100]}...")
            except:
                print(f"   {Fore.YELLOW}⚠️ MongoDB check skipped{Style.RESET_ALL}")
            
            # Recent log entries
            print(f"\n📝 {Fore.BLUE}Recent Log Entries:{Style.RESET_ALL}")
            if os.path.exists("scraper.log"):
                try:
                    with open("scraper.log", "r", encoding='utf-8') as f:
                        lines = f.readlines()[-10:]  # Last 10 lines
                    
                    for line in lines[-5:]:  # Show last 5
                        line = line.strip()
                        if "ERROR" in line or "❌" in line:
                            print(f"   {Fore.RED}{line[:100]}{Style.RESET_ALL}")
                        elif "SUCCESS" in line or "✅" in line:
                            print(f"   {Fore.GREEN}{line[:100]}{Style.RESET_ALL}")
                        elif "WARNING" in line or "⚠️" in line:
                            print(f"   {Fore.YELLOW}{line[:100]}{Style.RESET_ALL}")
                        else:
                            print(f"   {line[:100]}")
                except:
                    print(f"   {Fore.YELLOW}Cannot read log file{Style.RESET_ALL}")
            else:
                print(f"   {Fore.YELLOW}No log file found{Style.RESET_ALL}")
            
            # Commands
            print(f"\n🎮 {Fore.BLUE}Commands:{Style.RESET_ALL}")
            print(f"   {Fore.GREEN}[T]{Style.RESET_ALL} - Trigger scraping")
            print(f"   {Fore.GREEN}[S]{Style.RESET_ALL} - Show system status")
            print(f"   {Fore.GREEN}[L]{Style.RESET_ALL} - View full logs")
            print(f"   {Fore.GREEN}[R]{Style.RESET_ALL} - Refresh now")
            print(f"   {Fore.RED}[Q]{Style.RESET_ALL} - Quit monitor")
            
            print(Fore.CYAN + "\n" + "=" * 70 + Style.RESET_ALL)
            print("Press a command key or wait 10 seconds for auto-refresh...")
            
            # Check for user input (Windows compatible)
            start_time = time.time()
            user_input = None
            
            while time.time() - start_time < 10:  # Wait up to 10 seconds
                if has_msvcrt:
                    if msvcrt.kbhit():
                        user_input = msvcrt.getch().decode('utf-8').lower()
                        break
                else:
                    # Fallback for non-Windows or if msvcrt not available
                    try:
                        import threading
                        
                        def get_input():
                            nonlocal user_input
                            user_input = input().strip().lower()
                        
                        input_thread = threading.Thread(target=get_input)
                        input_thread.daemon = True
                        input_thread.start()
                        input_thread.join(0.1)
                    except:
                        pass
                
                time.sleep(0.1)
            
            # Process user input
            if user_input == 'q':
                print(f"\n{Fore.YELLOW}Exiting monitor...{Style.RESET_ALL}")
                break
            elif user_input == 't':
                print(f"\n{Fore.YELLOW}Triggering scraping...{Style.RESET_ALL}")
                trigger_ok, trigger_data = self.trigger_scrape()
                if trigger_ok:
                    print(f"{Fore.GREEN}✅ Scraping triggered successfully!{Style.RESET_ALL}")
                    print(f"Message: {trigger_data.get('message', '')}")
                    print(f"Timestamp: {trigger_data.get('timestamp', '')}")
                else:
                    print(f"{Fore.RED}❌ Failed to trigger scraping{Style.RESET_ALL}")
                input(f"\n{Fore.YELLOW}Press Enter to continue...{Style.RESET_ALL}")
            elif user_input == 's':
                self.show_system_status()
                input(f"\n{Fore.YELLOW}Press Enter to continue...{Style.RESET_ALL}")
            elif user_input == 'l':
                self.show_logs()
                input(f"\n{Fore.YELLOW}Press Enter to continue...{Style.RESET_ALL}")
            elif user_input == 'r':
                print(f"\n{Fore.YELLOW}Refreshing...{Style.RESET_ALL}")
                time.sleep(1)
                continue
            else:
                # Auto-refresh
                print(f"\n{Fore.YELLOW}Auto-refreshing in 3 seconds...{Style.RESET_ALL}")
                time.sleep(3)
    
    def show_system_status(self):
        """Show detailed system status."""
        self.clear_screen()
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)
        print("🔧 System Status Details")
        print("=" * 70 + Style.RESET_ALL)
        
        # Check if FastAPI is running
        print(f"\n{Fore.BLUE}FastAPI Server:{Style.RESET_ALL}")
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if proc.info['cmdline'] and 'run.py' in ' '.join(proc.info['cmdline']):
                    print(f"   {Fore.GREEN}✅ Running (PID: {proc.info['pid']}){Style.RESET_ALL}")
                    break
            else:
                print(f"   {Fore.RED}❌ Not running{Style.RESET_ALL}")
        except:
            print(f"   {Fore.YELLOW}⚠️ Cannot check process{Style.RESET_ALL}")
        
        # Check ports
        print(f"\n{Fore.BLUE}Port 8000:{Style.RESET_ALL}")
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 8000))
            if result == 0:
                print(f"   {Fore.GREEN}✅ Port is open{Style.RESET_ALL}")
            else:
                print(f"   {Fore.RED}❌ Port is closed{Style.RESET_ALL}")
            sock.close()
        except:
            print(f"   {Fore.YELLOW}⚠️ Cannot check port{Style.RESET_ALL}")
        
        # Check Python environment
        print(f"\n{Fore.BLUE}Python Environment:{Style.RESET_ALL}")
        print(f"   Python version: {sys.version.split()[0]}")
        print(f"   Working directory: {os.getcwd()}")
        
        # Check dependencies
        print(f"\n{Fore.BLUE}Key Dependencies:{Style.RESET_ALL}")
        try:
            import fastapi
            print(f"   FastAPI: {fastapi.__version__}")
        except:
            print(f"   {Fore.RED}FastAPI: NOT INSTALLED{Style.RESET_ALL}")
        
        try:
            import selenium
            print(f"   Selenium: {selenium.__version__}")
        except:
            print(f"   {Fore.RED}Selenium: NOT INSTALLED{Style.RESET_ALL}")
        
        try:
            import pymongo
            print(f"   PyMongo: {pymongo.__version__}")
        except:
            print(f"   {Fore.RED}PyMongo: NOT INSTALLED{Style.RESET_ALL}")
        
        # Disk space
        print(f"\n{Fore.BLUE}Disk Space:{Style.RESET_ALL}")
        try:
            import shutil
            total, used, free = shutil.disk_usage(".")
            print(f"   Total: {total // (2**30)} GB")
            print(f"   Used: {used // (2**30)} GB")
            print(f"   Free: {free // (2**30)} GB")
        except:
            print(f"   {Fore.YELLOW}⚠️ Cannot check disk space{Style.RESET_ALL}")
    
    def show_logs(self, num_lines=50):
        """Show recent logs."""
        self.clear_screen()
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)
        print("📋 Recent Logs (last 50 lines)")
        print("=" * 70 + Style.RESET_ALL)
        
        if os.path.exists("scraper.log"):
            try:
                with open("scraper.log", "r", encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Show last num_lines
                start_line = max(0, len(lines) - num_lines)
                for i in range(start_line, len(lines)):
                    line = lines[i].strip()
                    line_num = i + 1
                    
                    # Color code based on content
                    if "ERROR" in line or "❌" in line:
                        print(f"{Fore.RED}{line_num:4d}: {line}{Style.RESET_ALL}")
                    elif "SUCCESS" in line or "✅" in line:
                        print(f"{Fore.GREEN}{line_num:4d}: {line}{Style.RESET_ALL}")
                    elif "WARNING" in line or "⚠️" in line:
                        print(f"{Fore.YELLOW}{line_num:4d}: {line}{Style.RESET_ALL}")
                    elif "INFO" in line:
                        print(f"{Fore.CYAN}{line_num:4d}: {line}{Style.RESET_ALL}")
                    else:
                        print(f"{line_num:4d}: {line}")
            except Exception as e:
                print(f"{Fore.RED}Error reading log file: {e}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}No log file found{Style.RESET_ALL}")

def main():
    """Main function."""
    print(Fore.CYAN + Style.BRIGHT + "🚗 AutoScout24 Scraper Monitor - Windows Edition")
    print("Initializing..." + Style.RESET_ALL)
    
    # Check if colorama is installed
    try:
        import colorama
    except ImportError:
        print(f"{Fore.RED}Colorama is not installed. Installing...{Style.RESET_ALL}")
        os.system("pip install colorama")
        import colorama
        colorama.init()
    
    monitor = WindowsScraperMonitor()
    
    try:
        monitor.display_dashboard()
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Monitor stopped by user.{Style.RESET_ALL}")
    except Exception as e:
        print(f"\n{Fore.RED}Error in monitor: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Install required packages if missing
    required_packages = ['colorama', 'requests']
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing {package}...")
            os.system(f"pip install {package}")
    
    main()