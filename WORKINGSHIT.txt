import customtkinter as ctk
from tkinter import messagebox
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from bs4 import BeautifulSoup
import time
import re
import threading
import random
import sys
from PIL import Image
import os
import concurrent.futures
from queue import Queue
import requests
from urllib.parse import urljoin
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import lxml
from functools import lru_cache
import gc

# Set the appearance mode and color theme
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

class UltraFastBasketballScraper:
    def __init__(self, root):
        self.root = root
        self.root.title("Basketball Box Score Scraper - ULTRA FAST")
        self.root.geometry("1200x800")
        self.root.configure(bg="#06072e")
        
        # NBA Teams
        self.nba_teams = {
            'Atlanta Hawks': 'ATL',
            'Boston Celtics': 'BOS',
            'Brooklyn Nets': 'BRK',
            'Charlotte Hornets': 'CHO',
            'Chicago Bulls': 'CHI',
            'Cleveland Cavaliers': 'CLE',
            'Dallas Mavericks': 'DAL',
            'Denver Nuggets': 'DEN',
            'Detroit Pistons': 'DET',
            'Golden State Warriors': 'GSW',
            'Houston Rockets': 'HOU',
            'Indiana Pacers': 'IND',
            'LA Clippers': 'LAC',
            'Los Angeles Lakers': 'LAL',
            'Memphis Grizzlies': 'MEM',
            'Miami Heat': 'MIA',
            'Milwaukee Bucks': 'MIL',
            'Minnesota Timberwolves': 'MIN',
            'New Orleans Pelicans': 'NOP',
            'New York Knicks': 'NYK',
            'Oklahoma City Thunder': 'OKC',
            'Orlando Magic': 'ORL',
            'Philadelphia 76ers': 'PHI',
            'Phoenix Suns': 'PHX',
            'Portland Trail Blazers': 'POR',
            'Sacramento Kings': 'SAC',
            'San Antonio Spurs': 'SAS',
            'Toronto Raptors': 'TOR',
            'Utah Jazz': 'UTA',
            'Washington Wizards': 'WAS'
        }
        
        # WNBA Teams
        self.wnba_teams = {
            'Atlanta Dream': 'ATL',
            'Chicago Sky': 'CHI',
            'Connecticut Sun': 'CON',
            'Dallas Wings': 'DAL',
            'Golden State Valkyries': 'GSV',
            'Indiana Fever': 'IND',
            'Las Vegas Aces': 'LVA',
            'Los Angeles Sparks': 'LAS',
            'Minnesota Lynx': 'MIN',
            'New York Liberty': 'NYL',
            'Phoenix Mercury': 'PHX',
            'Seattle Storm': 'SEA',
            'Washington Mystics': 'WAS'
        }
        
        self.all_teams = {**self.nba_teams, **self.wnba_teams}
        self.selected_league = "NBA"
        self.selected_browser = "Chrome"
        self.log_queue = Queue()
        self.lebron_image_label = None
        
        # Ultra-fast optimization variables
        self.session = None
        self.driver_pool = []
        self.use_requests_fallback = True
        self.cache = {}
        
        # Initialize high-performance session
        self.init_ultra_fast_session()
        self.setup_ui()
        
    def init_ultra_fast_session(self):
        """Initialize ultra-fast requests session with connection pooling"""
        self.session = requests.Session()
        
        # Ultra-aggressive retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False
        )
        
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Ultra-fast headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def on_league_change(self, league):
        """Handle league selection change"""
        self.selected_league = league
        
        # Update button appearances
        if league == "NBA":
            self.nba_button.configure(fg_color="#FFFFFF", text_color="#06072e")
            self.wnba_button.configure(fg_color=("#3D316E", "#72618E"), text_color="#FFFFFF")
            teams = list(self.nba_teams.keys())
        else:
            self.wnba_button.configure(fg_color="#FFFFFF", text_color="#06072e")
            self.nba_button.configure(fg_color=("#3D316E", "#72618E"), text_color="#FFFFFF")
            teams = list(self.wnba_teams.keys())
        
        # Update team dropdowns
        self.team1_combo.configure(values=teams)
        self.team2_combo.configure(values=teams)
        
        # Clear current selections
        self.team1_combo.set("")
        self.team2_combo.set("")
        
    def on_browser_change(self, browser):
        """Handle browser selection change"""
        self.selected_browser = browser
        self.log(f"Selected browser: {browser}")
        
        # Update button appearances
        browsers = ["Chrome", "Firefox", "Edge"]
        for btn_browser in browsers:
            button = getattr(self, f"{btn_browser.lower()}_button")
            if btn_browser == browser:
                button.configure(fg_color="#FFFFFF", text_color="#06072e")
            else:
                button.configure(fg_color=("#3D316E", "#72618E"), text_color="#FFFFFF")
        
        # Update LeBron image based on browser selection
        self.update_lebron_image()
        
    def setup_ui(self):
        # Create scrollable frame
        self.scrollable_frame = ctk.CTkScrollableFrame(self.root, fg_color="#06072e")
        self.scrollable_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Main container frame (now inside scrollable frame)
        main_frame = ctk.CTkFrame(self.scrollable_frame, fg_color="#06072e")
        main_frame.pack(fill="both", expand=True)
        
        # League selection frame
        league_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        league_frame.pack(pady=10)
        
        league_label = ctk.CTkLabel(
            league_frame, 
            text="Select League:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        league_label.pack(pady=(0, 10))
        
        # League buttons frame
        league_buttons_frame = ctk.CTkFrame(league_frame, fg_color="transparent")
        league_buttons_frame.pack()
        
        self.nba_button = ctk.CTkButton(
            league_buttons_frame,
            text="NBA",
            width=100,
            height=40,
            corner_radius=20,
            fg_color="#FFFFFF",
            text_color="#06072e",
            hover_color="#FFFFFF",
            font=ctk.CTkFont(size=14, weight="bold"),
            command=lambda: self.on_league_change("NBA")
        )
        self.nba_button.pack(side="left", padx=(0, 10))
        
        self.wnba_button = ctk.CTkButton(
            league_buttons_frame,
            text="WNBA",
            width=100,
            height=40,
            corner_radius=20,
            fg_color=("#3D316E", "#72618E"),
            text_color="#FFFFFF",
            hover_color="#FFFFFF",
            font=ctk.CTkFont(size=14, weight="bold"),
            command=lambda: self.on_league_change("WNBA")
        )
        self.wnba_button.pack(side="left")
        
        # Browser selection frame
        browser_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        browser_frame.pack(pady=10)
        
        browser_label = ctk.CTkLabel(
            browser_frame, 
            text="Select Browser:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        browser_label.pack(pady=(0, 10))
        
        # Browser buttons frame
        browser_buttons_frame = ctk.CTkFrame(browser_frame, fg_color="transparent")
        browser_buttons_frame.pack()
        
        self.chrome_button = ctk.CTkButton(
            browser_buttons_frame,
            text="Chrome",
            width=80,
            height=35,
            corner_radius=15,
            fg_color="#FFFFFF",
            text_color="#06072e",
            hover_color="#FFFFFF",
            font=ctk.CTkFont(size=12, weight="bold"),
            command=lambda: self.on_browser_change("Chrome")
        )
        self.chrome_button.pack(side="left", padx=(0, 5))
        
        self.firefox_button = ctk.CTkButton(
            browser_buttons_frame,
            text="Firefox",
            width=80,
            height=35,
            corner_radius=15,
            fg_color=("#3D316E", "#72618E"),
            text_color="#FFFFFF",
            hover_color="#FFFFFF",
            font=ctk.CTkFont(size=12, weight="bold"),
            command=lambda: self.on_browser_change("Firefox")
        )
        self.firefox_button.pack(side="left", padx=5)
        
        self.edge_button = ctk.CTkButton(
            browser_buttons_frame,
            text="Edge/Opera GX",
            width=110,
            height=35,
            corner_radius=15,
            fg_color=("#3D316E", "#72618E"),
            text_color="#FFFFFF",
            hover_color="#FFFFFF",
            font=ctk.CTkFont(size=11, weight="bold"),
            command=lambda: self.on_browser_change("Edge")
        )
        self.edge_button.pack(side="left", padx=(5, 0))
        
        # Ultra Fast Mode Toggle
        self.ultra_fast_var = ctk.BooleanVar(value=True)
        self.ultra_fast_checkbox = ctk.CTkCheckBox(
            main_frame,
            text="🚀 ULTRA FAST MODE (Uses Requests instead of Browser)",
            variable=self.ultra_fast_var,
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#00FF00"
        )
        self.ultra_fast_checkbox.pack(pady=10)
        
        # Single centered LeBron image
        self.load_lebron_image(main_frame)
        
        # Form section - Teams
        form_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        form_frame.pack(pady=20)
        
        # Teams container - horizontal layout
        teams_frame = ctk.CTkFrame(form_frame, fg_color="transparent")
        teams_frame.pack(pady=(0, 15))
        
        # Team 1 selection
        team1_frame = ctk.CTkFrame(teams_frame, fg_color="transparent")
        team1_frame.pack(side="left", padx=(0, 20))
        
        team1_label = ctk.CTkLabel(
            team1_frame, 
            text="Team 1:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        team1_label.pack(pady=(0, 5))
        
        self.team1_combo = ctk.CTkComboBox(
            team1_frame,
            values=list(self.nba_teams.keys()),
            width=250,
            font=ctk.CTkFont(size=12)
        )
        self.team1_combo.pack()
        
        # Team 2 selection
        team2_frame = ctk.CTkFrame(teams_frame, fg_color="transparent")
        team2_frame.pack(side="right", padx=(20, 0))
        
        team2_label = ctk.CTkLabel(
            team2_frame, 
            text="Team 2:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        team2_label.pack(pady=(0, 5))
        
        self.team2_combo = ctk.CTkComboBox(
            team2_frame,
            values=list(self.nba_teams.keys()),
            width=250,
            font=ctk.CTkFont(size=12)
        )
        self.team2_combo.pack()
        
        # Season year
        season_label = ctk.CTkLabel(
            form_frame, 
            text="Season Year:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        season_label.pack(pady=(0, 5))
        
        self.season_entry = ctk.CTkEntry(
            form_frame,
            width=100,
            font=ctk.CTkFont(size=12)
        )
        self.season_entry.insert(0, "2025")
        self.season_entry.pack(pady=(0, 20))
        
        # Main action button
        self.get_data_btn = ctk.CTkButton(
            main_frame,
            text="⚡ GET LAST 5 GAMES ⚡",
            width=200,
            height=40,
            corner_radius=20,
            fg_color=("#FF6600", "#FF8800"),
            text_color="#FFFFFF",
            hover_color="#FF4400",
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self.get_data_threaded
        )
        self.get_data_btn.pack(pady=20)
        
        # Progress indicator
        self.progress_label = ctk.CTkLabel(
            main_frame,
            text="",
            font=ctk.CTkFont(size=12),
            text_color="#00FF00"
        )
        self.progress_label.pack(pady=(0, 10))
        
        # Results section - Box scores side by side
        results_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        results_frame.pack(fill="both", expand=True, pady=10)
        
        # Box scores container
        box_scores_frame = ctk.CTkFrame(results_frame, fg_color="transparent")
        box_scores_frame.pack(fill="both", expand=True)
        
        # Team 1 results
        team1_results_frame = ctk.CTkFrame(box_scores_frame, fg_color="transparent")
        team1_results_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))
        
        team1_label = ctk.CTkLabel(
            team1_results_frame, 
            text="Team 1 Box Scores:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        team1_label.pack(pady=(0, 5))
        
        self.team1_text = ctk.CTkTextbox(
            team1_results_frame,
            height=300,
            fg_color="#1e1f4d",
            text_color="#FFFFFF",
            font=ctk.CTkFont(size=10)
        )
        self.team1_text.pack(fill="both", expand=True)
        
        # Team 2 results
        team2_results_frame = ctk.CTkFrame(box_scores_frame, fg_color="transparent")
        team2_results_frame.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        team2_label = ctk.CTkLabel(
            team2_results_frame, 
            text="Team 2 Box Scores:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#FFFFFF"
        )
        team2_label.pack(pady=(0, 5))
        
        self.team2_text = ctk.CTkTextbox(
            team2_results_frame,
            height=300,
            fg_color="#1e1f4d",
            text_color="#FFFFFF",
            font=ctk.CTkFont(size=10)
        )
        self.team2_text.pack(fill="both", expand=True)
        
        # Log section - Full width underneath
        log_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        log_frame.pack(fill="x", pady=(15, 0))
        
        log_label = ctk.CTkLabel(
            log_frame, 
            text="🚀 Ultra Fast Log:", 
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#00FF00"
        )
        log_label.pack(pady=(0, 5))
        
        self.log_text = ctk.CTkTextbox(
            log_frame,
            height=120,
            fg_color="#2c2d59",
            text_color="#00FF00",
            font=ctk.CTkFont(size=10)
        )
        self.log_text.pack(fill="x")
        
        # Start log queue processor
        self.process_log_queue()
        
    def load_lebron_image(self, parent_frame):
        """Load and display centered LeBron image scaled to 75%"""
        try:
            # Determine image filename based on selected browser
            if self.selected_browser == "Chrome":
                image_filename = "lebron_chrome.png"
            elif self.selected_browser == "Firefox":
                image_filename = "lebron_firefox.png"
            elif self.selected_browser == "Edge":
                image_filename = "lebron_operaedge.png"
            else:
                image_filename = "lebron_chrome.png"  # Default fallback
            
            # Get the correct path for both development and PyInstaller bundle
            if hasattr(sys, '_MEIPASS'):
                # Running as PyInstaller bundle
                image_path = os.path.join(sys._MEIPASS, image_filename)
            else:
                # Running as script
                script_dir = os.path.dirname(os.path.abspath(__file__))
                image_path = os.path.join(script_dir, image_filename)
            
            if os.path.exists(image_path):
                # Load and resize image to 75% (522x394 -> 391x295)
                pil_image = Image.open(image_path)
                resized_image = pil_image.resize((391, 295), Image.Resampling.LANCZOS)
                
                # Convert to CTkImage
                ctk_image = ctk.CTkImage(
                    light_image=resized_image,
                    dark_image=resized_image,
                    size=(391, 295)
                )
                
                # Create centered image label
                self.lebron_image_label = ctk.CTkLabel(
                    parent_frame,
                    image=ctk_image,
                    text=""
                )
                self.lebron_image_label.pack(pady=10)
                
            else:
                self.log(f"{image_filename} not found at {image_path}")
                
        except Exception as e:
            self.log(f"Error loading LeBron image: {str(e)}")
    
    def update_lebron_image(self):
        """Update LeBron image based on current browser selection"""
        if self.lebron_image_label is None:
            return
            
        try:
            # Determine image filename based on selected browser
            if self.selected_browser == "Chrome":
                image_filename = "lebron_chrome.png"
            elif self.selected_browser == "Firefox":
                image_filename = "lebron_firefox.png"
            elif self.selected_browser == "Edge":
                image_filename = "lebron_operaedge.png"
            else:
                image_filename = "lebron_chrome.png"  # Default fallback
            
            # Get the correct path for both development and PyInstaller bundle
            if hasattr(sys, '_MEIPASS'):
                # Running as PyInstaller bundle
                image_path = os.path.join(sys._MEIPASS, image_filename)
            else:
                # Running as script
                script_dir = os.path.dirname(os.path.abspath(__file__))
                image_path = os.path.join(script_dir, image_filename)
            
            if os.path.exists(image_path):
                # Load and resize image to 75% (522x394 -> 391x295)
                pil_image = Image.open(image_path)
                resized_image = pil_image.resize((391, 295), Image.Resampling.LANCZOS)
                
                # Convert to CTkImage
                ctk_image = ctk.CTkImage(
                    light_image=resized_image,
                    dark_image=resized_image,
                    size=(391, 295)
                )
                
                # Update the existing label with new image
                self.lebron_image_label.configure(image=ctk_image)
                
            else:
                self.log(f"{image_filename} not found at {image_path}")
                
        except Exception as e:
            self.log(f"Error updating LeBron image: {str(e)}")
        
    def log(self, message):
        """Thread-safe logging method"""
        self.log_queue.put(message)
        
    def process_log_queue(self):
        """Process log messages from the queue (runs on main thread)"""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.log_text.insert("end", f"{message}\n")
                self.log_text.see("end")
        except:
            pass
        
        # Schedule next check
        self.root.after(50, self.process_log_queue)  # Faster log updates
        
    def update_progress(self, text):
        """Update progress label"""
        self.progress_label.configure(text=text)
        self.root.update_idletasks()
        
    def get_data_threaded(self):
        """Run data collection in a separate thread to prevent GUI freezing"""
        thread = threading.Thread(target=self.get_data)
        thread.daemon = True
        thread.start()
        
    def get_data(self):
        """ULTRA FAST main function with hybrid approach"""
        start_time = time.time()
        try:
            # Clear previous results
            self.team1_text.delete("1.0", "end")
            self.team2_text.delete("1.0", "end")
            self.log_text.delete("1.0", "end")
            self.update_progress("🚀 ULTRA FAST MODE INITIALIZING...")
            
            # Validate inputs
            team1 = self.team1_combo.get()
            team2 = self.team2_combo.get()
            season = self.season_entry.get()
            league = self.selected_league
            
            if not league:
                messagebox.showerror("Error", "Please select a league")
                return
                
            if not team1 or not team2 or not season:
                messagebox.showerror("Error", "Please select both teams and enter a season year")
                return
                
            if team1 == team2:
                messagebox.showerror("Error", "Please select different teams")
                return
                
            # Disable button during processing
            self.get_data_btn.configure(state='disabled')
            
            if self.ultra_fast_var.get():
                # ULTRA FAST MODE: Use async requests + parallel processing
                self.log("🚀 ULTRA FAST MODE ACTIVATED!")
                self.log("⚡ Using parallel async requests for maximum speed...")
                
                # Run both teams in parallel using async
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    future1 = executor.submit(self.ultra_fast_get_team_data, team1, season, 1)
                    future2 = executor.submit(self.ultra_fast_get_team_data, team2, season, 2)
                    
                    # Process results as they complete
                    for future in concurrent.futures.as_completed([future1, future2]):
                        team_name, box_scores = future.result()
                        
                        if team_name == team1:
                            text_widget = self.team1_text
                        else:
                            text_widget = self.team2_text
                            
                        if box_scores:
                            text_widget.insert("end", box_scores)
                            self.log(f"⚡ ULTRA FAST SUCCESS for {team_name}")
                        else:
                            text_widget.insert("end", f"No box scores found for {team_name}")
                            self.log(f"⚠ No data found for {team_name}")
            else:
                # Fallback to optimized browser mode
                self.log("🌐 Using optimized browser mode...")
                for i, team in enumerate([team1, team2], 1):
                    self.update_progress(f"Processing team {i}/2: {team}")
                    team_name, box_scores = self.get_team_box_scores(team, season, i)
                    
                    text_widget = self.team1_text if i == 1 else self.team2_text
                    if box_scores:
                        text_widget.insert("end", box_scores)
                        self.log(f"✓ Successfully retrieved box scores for {team}")
                    else:
                        text_widget.insert("end", f"No box scores found for {team}")
                        self.log(f"⚠ No box scores found for {team}")
                        
            elapsed_time = time.time() - start_time
            self.update_progress(f"🎉 COMPLETED IN {elapsed_time:.1f} SECONDS!")
            self.log(f"🏁 TOTAL TIME: {elapsed_time:.1f} seconds - ULTRA FAST!")
            
            # Cleanup
            gc.collect()
                    
        except Exception as e:
            self.log(f"❌ Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            self.update_progress("Error occurred")
        finally:
            # Re-enable button
            self.get_data_btn.configure(state='normal')
    
    @lru_cache(maxsize=100)
    def ultra_fast_get_team_data(self, team_name, season, team_number):
        """ULTRA FAST data retrieval using pure requests"""
        try:
            # Determine league and get team code
            if team_name in self.nba_teams:
                team_code = self.nba_teams[team_name]
                league = 'nba'
            elif team_name in self.wnba_teams:
                team_code = self.wnba_teams[team_name]
                league = 'wnba'
            else:
                self.log(f"Team {team_name} not found")
                return team_name, ""
            
            self.log(f"[ULTRA-{team_number}] ⚡ Starting {team_name}")
            
            # Step 1: Get schedule page ULTRA FAST
            schedule_url = f"https://www.basketball-reference.com/{league}/teams/{team_code}/{season}_games.html"
            
            response = self.session.get(schedule_url, timeout=10)
            response.raise_for_status()
            
            # Step 2: Parse with lxml (fastest parser)
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Step 3: Extract box score links ULTRA FAST
            box_score_links = self.ultra_fast_extract_links(soup)
            
            if not box_score_links:
                self.log(f"[ULTRA-{team_number}] No links found for {team_name}")
                return team_name, ""
            
            # Get last 5 games
            recent_links = box_score_links[-5:]
            self.log(f"[ULTRA-{team_number}] Found {len(recent_links)} games")
            
            # Step 4: Process games in parallel with requests
            all_box_scores = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for i, link in enumerate(recent_links, 1):
                    box_score_url = f"https://www.basketball-reference.com{link}"
                    future = executor.submit(self.ultra_fast_get_game_data, box_score_url, team_name, team_code, i, team_number)
                    futures.append(future)
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        game_data = future.result(timeout=15)
                        if game_data:
                            all_box_scores.append(game_data)
                    except Exception as e:
                        self.log(f"[ULTRA-{team_number}] Game error: {str(e)[:50]}...")
            
            self.log(f"[ULTRA-{team_number}] ✅ Completed {team_name}")
            return team_name, "\n\n".join(all_box_scores)
            
        except Exception as e:
            self.log(f"[ULTRA-{team_number}] ❌ Error for {team_name}: {str(e)[:50]}...")
            # Fallback to browser mode for this team
            return self.get_team_box_scores(team_name, season, team_number)
    
    def ultra_fast_get_game_data(self, url, team_name, team_code, game_num, team_number):
        """Get individual game data ULTRA FAST"""
        try:
            # Ultra-fast request with minimal timeout
            response = self.session.get(url, timeout=8)
            response.raise_for_status()
            
            # Parse with lxml for maximum speed
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Extract data super fast
            return self.ultra_fast_extract_box_score(soup, team_name, team_code)
            
        except Exception as e:
            self.log(f"[ULTRA-{team_number}] Game {game_num} failed: {str(e)[:30]}...")
            return ""
    
    def ultra_fast_extract_links(self, soup):
        """Extract box score links ULTRA FAST"""
        links = []
        
        # Multiple strategies for maximum speed
        strategies = [
            lambda: soup.select('a[href*="/boxscores/"]'),
            lambda: soup.find_all('a', href=re.compile(r'/boxscores/.*\.html')),
            lambda: [a for a in soup.find_all('a', href=True) if '/boxscores/' in a.get('href', '')]
        ]
        
        for strategy in strategies:
            try:
                elements = strategy()
                if elements:
                    links = [elem.get('href') for elem in elements if elem.get('href')]
                    break
            except:
                continue
                
        return list(set(links))  # Remove duplicates
    
    def ultra_fast_extract_box_score(self, soup, team_name, team_code):
        """Extract box score data ULTRA FAST"""
        try:
            # Multiple table ID strategies for speed
            table_ids = [
                f"box-{team_code}-game-basic",
                f"box_{team_code}_basic",
                f"box-{team_code.lower()}-game-basic",
                f"box_{team_code.lower()}_basic"
            ]
            
            table = None
            for table_id in table_ids:
                table = soup.find('table', id=table_id)
                if table:
                    break
            
            if not table:
                # Fallback: find any table with team code
                for t in soup.find_all('table'):
                    if t.get('id') and team_code.lower() in t.get('id').lower():
                        table = t
                        break
            
            if not table:
                return ""
            
            # Ultra-fast data extraction
            rows = table.find_all('tr')
            if not rows:
                return ""
            
            # Build output super efficiently
            lines = [f"{team_name} Basic Box Score Stats"]
            
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if cells:
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    if any(cell_texts):
                        lines.append('\t'.join(cell_texts))
            
            return '\n'.join(lines)
            
        except Exception as e:
            return ""
            
    def create_driver(self):
        """Create ultra-optimized webdriver for fallback"""
        try:
            if self.selected_browser == "Chrome":
                return self.create_ultra_chrome_driver()
            elif self.selected_browser == "Firefox":
                return self.create_ultra_firefox_driver()
            elif self.selected_browser == "Edge":
                return self.create_ultra_edge_driver()
            else:
                return self.create_ultra_chrome_driver()
        except Exception as e:
            self.log(f"Driver creation error: {str(e)}")
            raise
    
    def create_ultra_chrome_driver(self):
        """Create ULTRA optimized Chrome driver"""
        chrome_options = ChromeOptions()
        
        # Ultra-aggressive options for maximum speed
        ultra_args = [
            '--headless=new',  # New headless mode
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-features=VizDisplayCompositor',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-web-security',
            '--disable-features=TranslateUI',
            '--disable-extensions',
            '--disable-component-extensions-with-background-pages',
            '--disable-default-apps',
            '--disable-plugins',
            '--disable-images',
            '--disable-javascript',
            '--disable-java',
            '--disable-notifications',
            '--disable-popup-blocking',
            '--disable-background-networking',
            '--disable-sync',
            '--disable-translate',
            '--hide-scrollbars',
            '--mute-audio',
            '--no-first-run',
            '--disable-logging',
            '--disable-gpu-logging',
            '--silent',
            '--log-level=3',
            '--window-size=1920,1080',
            '--user-data-dir=/tmp/chrome-ultra-fast',
            '--aggressive-cache-discard',
            '--memory-pressure-off',
            '--max_old_space_size=1024',
            '--remote-debugging-port=0',
            '--disable-dev-tools',
            '--disable-background-mode',
            '--disable-client-side-phishing-detection',
            '--disable-component-update',
            '--disable-hang-monitor',
            '--disable-prompt-on-repost',
            '--disable-domain-reliability',
            '--disable-features=AudioServiceOutOfProcess',
            '--blink-settings=imagesEnabled=false',
            '--disable-ipc-flooding-protection'
        ]
        
        for arg in ultra_args:
            chrome_options.add_argument(arg)
        
        # Ultra-fast experimental options
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option('detach', True)
        
        # Ultra-fast prefs
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,
                'plugins': 2,
                'popups': 2,
                'geolocation': 2,
                'notifications': 2,
                'media_stream': 2,
            },
            'profile.managed_default_content_settings': {
                'images': 2
            }
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        return webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=chrome_options
        )
    
    def create_ultra_firefox_driver(self):
        """Create ULTRA optimized Firefox driver"""
        firefox_options = FirefoxOptions()
        firefox_options.add_argument('--headless')
        
        # Ultra-fast Firefox preferences
        ultra_prefs = {
            'permissions.default.image': 2,
            'javascript.enabled': False,
            'browser.cache.disk.enable': False,
            'browser.cache.memory.enable': False,
            'browser.cache.offline.enable': False,
            'network.http.use-cache': False,
            'media.autoplay.enabled': False,
            'media.autoplay.default': 5,
            'browser.privatebrowsing.autostart': True,
            'dom.webdriver.enabled': False,
            'useAutomationExtension': False,
        }
        
        for pref, value in ultra_prefs.items():
            firefox_options.set_preference(pref, value)
        
        return webdriver.Firefox(
            service=FirefoxService(GeckoDriverManager().install()),
            options=firefox_options
        )
    
    def create_ultra_edge_driver(self):
        """Create ULTRA optimized Edge driver"""
        edge_options = EdgeOptions()
        
        # Similar ultra-aggressive options as Chrome
        ultra_args = [
            '--headless=new',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-images',
            '--disable-javascript',
            '--disable-plugins',
            '--disable-extensions',
            '--aggressive-cache-discard',
            '--memory-pressure-off',
            '--remote-debugging-port=0'
        ]
        
        for arg in ultra_args:
            edge_options.add_argument(arg)
        
        edge_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        edge_options.add_experimental_option('useAutomationExtension', False)
        
        return webdriver.Edge(
            service=EdgeService(EdgeChromiumDriverManager().install()),
            options=edge_options
        )
    
    def get_team_box_scores(self, team_name, season, team_number):
        """Fallback browser method with optimizations"""
        # Determine league and get team code
        if team_name in self.nba_teams:
            team_code = self.nba_teams[team_name]
            league = 'nba'
        elif team_name in self.wnba_teams:
            team_code = self.wnba_teams[team_name]
            league = 'wnba'
        else:
            self.log(f"Team {team_name} not found in any league")
            return team_name, ""
        
        driver = None
        try:
            self.log(f"[Browser-{team_number}] Starting {team_name}")
            
            # Create ultra-optimized driver
            driver = self.create_driver()
            driver.set_page_load_timeout(15)
            driver.implicitly_wait(3)
            
            # Get schedule page
            schedule_url = f"https://www.basketball-reference.com/{league}/teams/{team_code}/{season}_games.html"
            driver.get(schedule_url)
            
            # Quick wait
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Parse and extract
            soup = BeautifulSoup(driver.page_source, 'lxml')
            box_score_links = self.ultra_fast_extract_links(soup)
            
            if not box_score_links:
                return team_name, ""
            
            recent_links = box_score_links[-5:]
            all_box_scores = []
            
            # Process games quickly
            for i, link in enumerate(recent_links, 1):
                try:
                    box_score_url = f"https://www.basketball-reference.com{link}"
                    driver.get(box_score_url)
                    
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    game_soup = BeautifulSoup(driver.page_source, 'lxml')
                    box_score_data = self.ultra_fast_extract_box_score(game_soup, team_name, team_code)
                    
                    if box_score_data:
                        all_box_scores.append(box_score_data)
                        self.log(f"[Browser-{team_number}] ✓ Game {i}")
                    
                except Exception as e:
                    self.log(f"[Browser-{team_number}] Game {i} failed")
                    continue
            
            return team_name, "\n\n".join(all_box_scores)
            
        except Exception as e:
            self.log(f"[Browser-{team_number}] ❌ Error: {str(e)[:50]}...")
            return team_name, ""
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

def main():
    root = ctk.CTk()
    app = UltraFastBasketballScraper(root)
    root.mainloop()

if __name__ == "__main__":
    main()