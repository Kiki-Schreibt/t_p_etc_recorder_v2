import re
""" License Information stylesheets
    Copyright (c) DevSec Studio. All rights reserved.
    https://qss-stock.devsecstudio.com/templates.php
    MIT License

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


def extract_button_color(stylesheet, widget_name="QPushButton", property_name="background-color"):
    # Pattern to capture the CSS block for QPushButton
    pattern = r"{widget_name}\s*\{{(.*?)\}}".format(widget_name=widget_name)

    # Find all matches for QPushButton CSS blocks
    widget_blocks = re.findall(pattern, stylesheet, re.DOTALL)

    if not widget_blocks:
        return None  # Return None if no blocks are found

    # For each block, search for the background-color property
    for block in widget_blocks:
        # Find the background-color line
        color_pattern = r"{property_name}\s*:\s*(.*?);".format(property_name=property_name)
        color_match = re.search(color_pattern, block, re.IGNORECASE)

        if color_match:
            # Return the color value if found
            return color_match.group(1).strip()

    return None  # Return None if no color is found

takezo = """
            QWidget
        {
            background-color: #17212b;
            color: #fff;
        
        }
        
        
        /*-----QLabel-----*/
        QLabel
        {
            background-color: transparent;
            color: #fff;
        
        }
        
        
        /*-----QMenuBar-----*/
        QMenuBar 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(23, 43, 68, 255),stop:1 rgba(33, 51, 75, 255));
            border: none;
            color: #fff;
        
        }
        
        
        QMenuBar::item 
        {
            background-color: transparent;
        
        }
        
        
        QMenuBar::item:selected 
        {
            background-color: #24436b;
            color: #fff;
        
        }
        
        
        QMenuBar::item:pressed 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(36, 68, 108, 255),stop:1 rgba(50, 71, 100, 255));
            border: 1px solid #000;
            color: #fff;
        
        }
        
        
        /*-----QMenu-----*/
        QMenu
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border: 1px solid #222;
            color: #fff;
        
        }
        
        
        QMenu::item
        {
            background-color: transparent;
            padding: 4px;
            min-width: 100px;
        }
        
        
        QMenu::item:disabled
        {
            color: #555;
            background-color: transparent;
            padding: 2px 20px 2px 20px;
        
        }
        
        
        QMenu::item:selected
        {
            background-color: #344281;
            border: 1px solid #000;
            color: #fff;
        
        }
        
        
        /*-----QTabBar-----*/
        QTabBar 
        {
            background-color: transparent;
        }
        
        
        QTabWidget::tab-bar 
        {
            border:none;
            left: 0px;
        
        }
        
        
        QTabBar::tab 
        {
            color: #fff;
            padding-left: 15px; 
            padding-right: 15px; 
            height: 25px;
        }
        
        
        QTabWidget::pane 
        {
            border: 1px solid #3a546e; 
        
        }
        
        
        QTabBar::tab:!selected 
        {
            color: #b1b1b1; 
            border: px solid #000;
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(44, 52, 60, 255),stop:1 rgba(15, 20, 26, 255));
            margin-top: 3px;
        }
        
        
        QTabBar::tab:selected 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border: 1px solid #414141;
            color: #fff;
        
        }
        
        
        QTabBar::tab:!selected:hover 
        {
            color: #fff; 
        
        }
        
        
        /*-----QToolButton-----*/
        QToolButton 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(54, 66, 78, 255),stop:1 rgba(35, 48, 61, 255));
            color: #fff;
            border: 1px solid #3a546e; 
            padding: 2px;
            
        }
        
        
        QToolButton:hover
        {
            background-color: #43617f;
            
        }
        
        
        QToolButton:pressed
        {
            background-color: #27394a;
        
        }
        
        
        QToolButton:checked
        {
            background-color: #43617f;
        
        }
        
        
        /*-----QDockWidget-----*/
        QDockWidget::title 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border: 1px solid #3a3a3a;
            padding: 2px;
        
        }
        
        
        QDockWidget::close-button
        {
            max-width: 14px;
            max-height: 14px;
            margin-top:1px;
        
        }
        
        
        QDockWidget::float-button
        {
            max-width: 14px;
            max-height: 14px;
            margin-top:1px;
        
        }
        
        
        QDockWidget::close-button:hover
        {
            border: none;
            background-color: none;
        
        }
        
        
        QDockWidget::float-button:hover
        {
            border: none;
            background-color: none;
        
        }
        
        
        /*-----QTreeWidget-----*/
        QTreeWidget
        {
            show-decoration-selected: 0;
            selection-background-color: transparent; 
            selection-color: #fff;
            background-color: #1e2329;
            border: 1px solid gray;
            padding-top : 5px;
            color: #fff;
            font: 8pt;
        
        }
        
        
        QTreeView::item:selected
        {
            color:#fff;
            background-color: #344281;
            border-radius: 0px;
        
        }
        
        
        QTreeView::item:!selected:hover
        {
            background-color: #27394a;
            color: #fff;
        }
        
        
        QTreeView::branch:has-children:!has-siblings:closed,
        QTreeView::branch:closed:has-children:has-siblings 
        {
            image: url(://tree-closed.png);
        
        }
        
        
        QTreeView::branch:open:has-children:!has-siblings,
        QTreeView::branch:open:has-children:has-siblings  
        {
            image: url(://tree-open.png);
        
        }
        
        
        /*-----QLineEdit-----*/
        QLineEdit
        {
            background-color: #fdfdfd;
            color : black;
            border: 1px solid darkgray;
        
        }
        
        QLineEdit:hover 
        {
            background-color: rgb(190, 190, 190);
        
        }
        
        
        /*-----QGroupBox-----*/
        QGroupBox 
        {
            border: 0px solid #3a546e;
            background-color: #1e2329;
            margin-top: 5.5ex;
        
        }
        
        
        QGroupBox::title 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border: 1px solid #3a3a3a;
            subcontrol-origin: margin;
            subcontrol-position: top right 0px;
            border-radius: 0px;
            padding: 1px 30px;
            margin-bottom: 52px;
        
        }
        
        
        /*-----QComboBox-----*/
        QComboBox 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border: 1px solid #777777;
            color: #fff;
            padding: 2px;
        
        }
        
        
        QComboBox:editable 
        {
            background: transparent;
        
        }
        
        
        QComboBox::drop-down 
        {
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 15px;
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            border-left-color: #777777;
            border-left-style: solid; 
            border-top-right-radius: 3px; 
            border-bottom-right-radius: 3px;
        
        }
        
        
        QComboBox::down-arrow 
        {
            image: url(://arrow-down.png);
            width:8px;
            height:8px;
        
        }
        
        
        QComboBox::down-arrow:on 
        { 
            top: 1px;
            left: 1px;
        
        }
        
        
        QComboBox QListView
        {
            background-color: #17212b;
            border-left-style: solid; 
            selection-background-color: #344281;
            selection-color: #fff;
            color: #fff;
            border: 1px solid black;
            border-radius: 2px;
        
        }
        
        
        /*-----QSpinBox-----*/
        QSpinBox 
        {
            background-color: #fdfdfd;
            border: 1px solid gray;
            min-height: 12px;
            color : black;S
            padding: 2px;
        
        }
        
        
        QSpinBox:hover 
        {
            background-color: rgb(190, 190, 190);
        
        }
        
        
        QSpinBox::up-button 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            width: 16px; 
            border-width: 1px;
        
        }
        
        
        QSpinBox::up-button:hover 
        {
            background-color: #585858;
        
        }
        
        
        QSpinBox::up-button:pressed 
        {
            background-color: #252525;
            width: 16px; 
            border-width: 1px;
        
        }
        
        
        QSpinBox::up-arrow 
        {
            image: url(://arrow-up.png);
            width: 7px;
            height: 7px;
        
        }
        
        
        QSpinBox::down-button 
        {
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(67, 79, 91, 255),stop:1 rgba(32, 44, 56, 255));
            width: 16px; 
            border-width: 1px;
        
        }
        
        
        QSpinBox::down-button:hover 
        {
            background-color: #585858;
        
        }
        
        QSpinBox::down-button:pressed 
        {
            background-color: #252525;
            width: 16px; 
            border-width: 1px;
        
        }
        
        
        QSpinBox::down-arrow 
        {
            image: url(://arrow-down.png);
            width: 7px;
            height: 7px;
        
        }
        
        
        /*-----QCheckBox-----*/
        QCheckBox
        {
            background-color: transparent;
            color: #fff;
            border: none;
        
        }
        
        
        QCheckBox::indicator
        {
            color: #b1b1b1;
            background-color: #323232;
            border: 1px solid darkgray;
            width: 12px;
            height: 12px;
        
        }
        
        
        QCheckBox::indicator:checked
        {
            image:url("./ressources/check.png");
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(23, 43, 68, 255),stop:1 rgba(33, 51, 75, 255));
            border: 1px solid #3a546e;
        
        }
        
        
        QCheckBox::indicator:unchecked:hover
        {
            border: 1px solid #3a546e; 
        
        }
        
        
        QCheckBox::disabled
        {
            color: #656565;
        
        }
        
        
        QCheckBox::indicator:disabled
        {
            background-color: #656565;
            color: #656565;
            border: 1px solid #656565;
        
        }
        
        
        /*-----QRadioButton-----*/
        QRadioButton 
        {
            color: lightgray;
            background-color: transparent;
        
        }
        
        
        QRadioButton::indicator::unchecked:hover 
        {
            background-color: #fff;
            border: 2px solid #3a546e;
            border-radius: 6px;
        }
        
        
        QRadioButton::indicator::checked 
        {
            border: 2px solid #3a546e;
            border-radius: 6px;
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(23, 43, 68, 255),stop:1 rgba(33, 51, 75, 255)); 
            width: 9px; 
            height: 9px; 
        
        }
        
        
        /*-----QStatusBar-----*/
        QStatusBar 
        {
            color: #fff;
            background-color: qlineargradient(spread:repeat, x1:1, y1:0, x2:1, y2:1, stop:0 rgba(23, 43, 68, 255),stop:1 rgba(33, 51, 75, 255));
        
        }
        
        
        QStatusBar::item 
        {
            background-color: transparent;
            color: #fff;
        
        }
        
        
        /*-----QSizeGrip-----*/
        QSizeGrip 
        {
            background-color: image("./ressources/sizegrip.png"); /*To replace*/
        
        }
        
                    }
        """

toolery = """
            QWidget
            {
                background-color: #fff;
                color: red;
            
            }
            
            
            /*-----QLabel-----*/
            QLabel
            {
                background-color: transparent;
                color: #454544;
                font-weight: bold;
                font-size: 13px;
            
            }
            
            
            /*-----QPushButton-----*/
            QPushButton
            {
                background-color: #5c55e9;
                color: #fff;
                font-size: 13px;
                font-weight: bold;
                border-top-right-radius: 15px;
                border-top-left-radius: 0px;
                border-bottom-right-radius: 0px;
                border-bottom-left-radius: 15px;
                padding: 10px;
            
            }
            
            
            QPushButton::disabled
            {
                background-color: #5c5c5c;
            
            }
            
            
            QPushButton::hover
            {
                background-color: #5564f2;
            
            }
            
            
            QPushButton::pressed
            {
                background-color: #3d4ef2;
            
            }
            
            
            /*-----QCheckBox-----*/
            QCheckBox
            {
                background-color: transparent;
                color: #5c55e9;
                font-size: 10px;
                font-weight: bold;
                border: none;
                border-radius: 5px;
            
            }
            
            
            /*-----QCheckBox-----*/
            QCheckBox::indicator
            {
                background-color: #323232;
                border: 1px solid darkgray;
                width: 12px;
                height: 12px;
            
            }
        """


###ElegantDark Style Sheet for QT Applications
#Author: Jaime A. Quiroga P.
#Company: GTRONICK
#Last updated: 17/04/2018
#Available at: https://github.com/GTRONICK/QSS/blob/master/ElegantDark.qss
#*/

elegant_dark = """
QMainWindow {
	background-color:rgb(82, 82, 82);
}
QTextEdit {
	background-color:rgb(42, 42, 42);
	color: rgb(0, 255, 0);
}
QPushButton{
	border-style: outset;
	border-width: 2px;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-bottom-color: rgb(58, 58, 58);
	border-bottom-width: 1px;
	border-style: solid;
	color: rgb(255, 255, 255);
	padding: 2px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(77, 77, 77, 255), stop:1 rgba(97, 97, 97, 255));
}
QPushButton:hover{
	border-style: outset;
	border-width: 2px;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(180, 180, 180, 255), stop:1 rgba(110, 110, 110, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(180, 180, 180, 255), stop:1 rgba(110, 110, 110, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(180, 180, 180, 255), stop:1 rgba(110, 110, 110, 255));
	border-bottom-color: rgb(115, 115, 115);
	border-bottom-width: 1px;
	border-style: solid;
	color: rgb(255, 255, 255);
	padding: 2px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(107, 107, 107, 255), stop:1 rgba(157, 157, 157, 255));
}
QPushButton:pressed{
	border-style: outset;
	border-width: 2px;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(62, 62, 62, 255), stop:1 rgba(22, 22, 22, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-bottom-color: rgb(58, 58, 58);
	border-bottom-width: 1px;
	border-style: solid;
	color: rgb(255, 255, 255);
	padding: 2px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(77, 77, 77, 255), stop:1 rgba(97, 97, 97, 255));
}
QPushButton:disabled{
	border-style: outset;
	border-width: 2px;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-bottom-color: rgb(58, 58, 58);
	border-bottom-width: 1px;
	border-style: solid;
	color: rgb(0, 0, 0);
	padding: 2px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(57, 57, 57, 255), stop:1 rgba(77, 77, 77, 255));
}
QLineEdit {
	border-width: 1px; border-radius: 4px;
	border-color: rgb(58, 58, 58);
	border-style: inset;
	padding: 0 8px;
	color: rgb(255, 255, 255);
	background:rgb(100, 100, 100);
	selection-background-color: rgb(187, 187, 187);
	selection-color: rgb(60, 63, 65);
}
QLabel {
	color:rgb(255,255,255);	
}
QProgressBar {
	text-align: center;
	color: rgb(240, 240, 240);
	border-width: 1px; 
	border-radius: 10px;
	border-color: rgb(58, 58, 58);
	border-style: inset;
	background-color:rgb(77,77,77);
}
QProgressBar::chunk {
	background-color: qlineargradient(spread:pad, x1:0.5, y1:0.7, x2:0.5, y2:0.3, stop:0 rgba(87, 97, 106, 255), stop:1 rgba(93, 103, 113, 255));
	border-radius: 5px;
}
QMenuBar {
	background:rgb(82, 82, 82);
}
QMenuBar::item {
	color:rgb(223,219,210);
	spacing: 3px;
	padding: 1px 4px;
	background: transparent;
}

QMenuBar::item:selected {
	background:rgb(115, 115, 115);
}
QMenu::item:selected {
	color:rgb(255,255,255);
	border-width:2px;
	border-style:solid;
	padding-left:18px;
	padding-right:8px;
	padding-top:2px;
	padding-bottom:3px;
	background:qlineargradient(spread:pad, x1:0.5, y1:0.7, x2:0.5, y2:0.3, stop:0 rgba(87, 97, 106, 255), stop:1 rgba(93, 103, 113, 255));
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(62, 62, 62, 255));
	border-bottom-color: rgb(58, 58, 58);
	border-bottom-width: 1px;
}
QMenu::item {
	color:rgb(223,219,210);
	background-color:rgb(78,78,78);
	padding-left:20px;
	padding-top:4px;
	padding-bottom:4px;
	padding-right:10px;
}
QMenu{
	background-color:rgb(78,78,78);
}
QTabWidget {
	color:rgb(0,0,0);
	background-color:rgb(247,246,246);
}
QTabWidget::pane {
		border-color: rgb(77,77,77);
		background-color:rgb(101,101,101);
		border-style: solid;
		border-width: 1px;
    	border-radius: 6px;
}
QTabBar::tab {
	padding:2px;
	color:rgb(250,250,250);
  	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(77, 77, 77, 255), stop:1 rgba(97, 97, 97, 255));
	border-style: solid;
	border-width: 2px;
  	border-top-right-radius:4px;
   border-top-left-radius:4px;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:0.6, x2:0.5, y2:0.4, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(95, 92, 93, 255));
	border-right-color: qlineargradient(spread:pad, x1:0.4, y1:0.5, x2:0.6, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(95, 92, 93, 255));
	border-left-color: qlineargradient(spread:pad, x1:0.6, y1:0.5, x2:0.4, y2:0.5, stop:0 rgba(115, 115, 115, 255), stop:1 rgba(95, 92, 93, 255));
	border-bottom-color: rgb(101,101,101);
}
QTabBar::tab:selected, QTabBar::tab:last:selected, QTabBar::tab:hover {
  	background-color:rgb(101,101,101);
  	margin-left: 0px;
  	margin-right: 1px;
}
QTabBar::tab:!selected {
    	margin-top: 1px;
		margin-right: 1px;
}
QCheckBox {
	color:rgb(223,219,210);
	padding: 2px;
}
QCheckBox:hover {
	border-radius:4px;
	border-style:solid;
	padding-left: 1px;
	padding-right: 1px;
	padding-bottom: 1px;
	padding-top: 1px;
	border-width:1px;
	border-color: rgb(87, 97, 106);
	background-color:qlineargradient(spread:pad, x1:0.5, y1:0.7, x2:0.5, y2:0.3, stop:0 rgba(87, 97, 106, 150), stop:1 rgba(93, 103, 113, 150));
}
QCheckBox::indicator:checked {
	border-radius:4px;
	border-style:solid;
	border-width:1px;
	border-color: rgb(180,180,180);
  	background-color:qlineargradient(spread:pad, x1:0.5, y1:0.7, x2:0.5, y2:0.3, stop:0 rgba(87, 97, 106, 255), stop:1 rgba(93, 103, 113, 255));
}
QCheckBox::indicator:unchecked {
	border-radius:4px;
	border-style:solid;
	border-width:1px;
	border-color: rgb(87, 97, 106);
  	background-color:rgb(255,255,255);
}
QStatusBar {
	color:rgb(240,240,240);
}"""

#/*
#Aqua Style Sheet for QT Applications
#Author: Jaime A. Quiroga P.
#Company: GTRONICK
#Last updated: 22/01/2019, 07:55.
#Available at: https://github.com/GTRONICK/QSS/blob/master/Aqua.qss
#*/

aqua = """
QMainWindow {
	background-color:#ececec;
}
QTextEdit {
	border-width: 1px;
	border-style: solid;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QPlainTextEdit {
	border-width: 1px;
	border-style: solid;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QToolButton {
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(255,255,255);
}
QToolButton:hover{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(195, 195, 195), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(197, 197, 197), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(197, 197, 197));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(195, 195, 195), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(255,255,255);
}
QToolButton:pressed{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(142,142,142);
}
QPushButton{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(255,255,255);
}
QPushButton::default{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(255,255,255);
}
QPushButton:hover{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(195, 195, 195), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(197, 197, 197), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(197, 197, 197));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(195, 195, 195), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(255,255,255);
}
QPushButton:pressed{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: rgb(0,0,0);
	padding: 2px;
	background-color: rgb(142,142,142);
}
QPushButton:disabled{
	border-style: solid;
	border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-right-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(217, 217, 217), stop:1 rgb(227, 227, 227));
	border-left-color: qlineargradient(spread:pad, x1:0, y1:0.5, x2:1, y2:0.5, stop:0 rgb(227, 227, 227), stop:1 rgb(217, 217, 217));
	border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgb(215, 215, 215), stop:1 rgb(222, 222, 222));
	border-width: 1px;
	border-radius: 5px;
	color: #808086;
	padding: 2px;
	background-color: rgb(142,142,142);
}
QLineEdit {
	border-width: 1px; border-radius: 4px;
	border-style: solid;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QLabel {
	color: #000000;
}
QLCDNumber {
	color: rgb(0, 113, 255, 255);
}
QProgressBar {
	text-align: center;
	color: rgb(240, 240, 240);
	border-width: 1px; 
	border-radius: 10px;
	border-color: rgb(230, 230, 230);
	border-style: solid;
	background-color:rgb(207,207,207);
}
QProgressBar::chunk {
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(49, 147, 250, 255), stop:1 rgba(34, 142, 255, 255));
	border-radius: 10px;
}
QMenuBar {
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(207, 209, 207, 255), stop:1 rgba(230, 229, 230, 255));
}
QMenuBar::item {
	color: #000000;
  	spacing: 3px;
  	padding: 1px 4px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(207, 209, 207, 255), stop:1 rgba(230, 229, 230, 255));
}

QMenuBar::item:selected {
  	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	color: #FFFFFF;
}
QMenu::item:selected {
	border-style: solid;
	border-top-color: transparent;
	border-right-color: transparent;
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	border-bottom-color: transparent;
	border-left-width: 2px;
	color: #000000;
	padding-left:15px;
	padding-top:4px;
	padding-bottom:4px;
	padding-right:7px;
}
QMenu::item {
	border-style: solid;
	border-top-color: transparent;
	border-right-color: transparent;
	border-left-color: transparent;
	border-bottom-color: transparent;
	border-bottom-width: 1px;
	color: #000000;
	padding-left:17px;
	padding-top:4px;
	padding-bottom:4px;
	padding-right:7px;
}
QTabWidget {
	color:rgb(0,0,0);
	background-color:#000000;
}
QTabWidget::pane {
		border-color: rgb(223,223,223);
		background-color:rgb(226,226,226);
		border-style: solid;
		border-width: 2px;
    	border-radius: 6px;
}
QTabBar::tab:first {
	border-style: solid;
	border-left-width:1px;
	border-right-width:0px;
	border-top-width:1px;
	border-bottom-width:1px;
	border-top-color: rgb(209,209,209);
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-bottom-color: rgb(229,229,229);
	border-top-left-radius: 4px;
	border-bottom-left-radius: 4px;
	color: #000000;
	padding: 3px;
	margin-left:0px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(247, 247, 247, 255), stop:1 rgba(255, 255, 255, 255));
}
QTabBar::tab:last {
	border-style: solid;
	border-width:1px;
	border-top-color: rgb(209,209,209);
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-right-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-bottom-color: rgb(229,229,229);
	border-top-right-radius: 4px;
	border-bottom-right-radius: 4px;
	color: #000000;
	padding: 3px;
	margin-left:0px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(247, 247, 247, 255), stop:1 rgba(255, 255, 255, 255));
}
QTabBar::tab {
	border-style: solid;
	border-top-width:1px;
	border-bottom-width:1px;
	border-left-width:1px;
	border-top-color: rgb(209,209,209);
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-bottom-color: rgb(229,229,229);
	color: #000000;
	padding: 3px;
	margin-left:0px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(247, 247, 247, 255), stop:1 rgba(255, 255, 255, 255));
}
QTabBar::tab:selected, QTabBar::tab:last:selected, QTabBar::tab:hover {
  	border-style: solid;
  	border-left-width:1px;
	border-right-color: transparent;
	border-top-color: rgb(209,209,209);
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-bottom-color: rgb(229,229,229);
	color: #FFFFFF;
	padding: 3px;
	margin-left:0px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}

QTabBar::tab:selected, QTabBar::tab:first:selected, QTabBar::tab:hover {
  	border-style: solid;
  	border-left-width:1px;
  	border-bottom-width:1px;
  	border-top-width:1px;
	border-right-color: transparent;
	border-top-color: rgb(209,209,209);
	border-left-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(209, 209, 209, 209), stop:1 rgba(229, 229, 229, 229));
	border-bottom-color: rgb(229,229,229);
	color: #FFFFFF;
	padding: 3px;
	margin-left:0px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}

QCheckBox {
	color: #000000;
	padding: 2px;
}
QCheckBox:disabled {
	color: #808086;
	padding: 2px;
}

QCheckBox:hover {
	border-radius:4px;
	border-style:solid;
	padding-left: 1px;
	padding-right: 1px;
	padding-bottom: 1px;
	padding-top: 1px;
	border-width:1px;
	border-color: transparent;
}
QCheckBox::indicator:checked {

	height: 10px;
	width: 10px;
	border-style:solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	color: #000000;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QCheckBox::indicator:unchecked {

	height: 10px;
	width: 10px;
	border-style:solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	color: #000000;
}
QRadioButton {
	color: 000000;
	padding: 1px;
}
QRadioButton::indicator:checked {
	height: 10px;
	width: 10px;
	border-style:solid;
	border-radius:5px;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	color: #a9b7c6;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QRadioButton::indicator:!checked {
	height: 10px;
	width: 10px;
	border-style:solid;
	border-radius:5px;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
	color: #a9b7c6;
	background-color: transparent;
}
QStatusBar {
	color:#027f7f;
}
QSpinBox {
	border-style: solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QDoubleSpinBox {
	border-style: solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QTimeEdit {
	border-style: solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QDateTimeEdit {
	border-style: solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}
QDateEdit {
	border-style: solid;
	border-width: 1px;
	border-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(0, 113, 255, 255), stop:1 rgba(91, 171, 252, 255));
}

QToolBox {
	color: #a9b7c6;
	background-color:#000000;
}
QToolBox::tab {
	color: #a9b7c6;
	background-color:#000000;
}
QToolBox::tab:selected {
	color: #FFFFFF;
	background-color:#000000;
}
QScrollArea {
	color: #FFFFFF;
	background-color:#000000;
}
QSlider::groove:horizontal {
	height: 5px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(49, 147, 250, 255), stop:1 rgba(34, 142, 255, 255));
}
QSlider::groove:vertical {
	width: 5px;
	background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(49, 147, 250, 255), stop:1 rgba(34, 142, 255, 255));
}
QSlider::handle:horizontal {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(207,207,207);
	width: 12px;
	margin: -5px 0;
	border-radius: 7px;
}
QSlider::handle:vertical {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(207,207,207);
	height: 12px;
	margin: 0 -5px;
	border-radius: 7px;
}
QSlider::add-page:horizontal {
    background: rgb(181,181,181);
}
QSlider::add-page:vertical {
    background: rgb(181,181,181);
}
QSlider::sub-page:horizontal {
    background-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 rgba(49, 147, 250, 255), stop:1 rgba(34, 142, 255, 255));
}
QSlider::sub-page:vertical {
    background-color: qlineargradient(spread:pad, y1:0.5, x1:1, y2:0.5, x2:0, stop:0 rgba(49, 147, 250, 255), stop:1 rgba(34, 142, 255, 255));
}
QScrollBar:horizontal {
	max-height: 20px;
	border: 1px transparent grey;
	margin: 0px 20px 0px 20px;
}
QScrollBar:vertical {
	max-width: 20px;
	border: 1px transparent grey;
	margin: 20px 0px 20px 0px;
}
QScrollBar::handle:horizontal {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(207,207,207);
	border-radius: 7px;
	min-width: 25px;
}
QScrollBar::handle:horizontal:hover {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(147, 200, 200);
	border-radius: 7px;
	min-width: 25px;
}
QScrollBar::handle:vertical {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(207,207,207);
	border-radius: 7px;
	min-height: 25px;
}
QScrollBar::handle:vertical:hover {
	background: rgb(253,253,253);
	border-style: solid;
	border-width: 1px;
	border-color: rgb(147, 200, 200);
	border-radius: 7px;
	min-height: 25px;
}
QScrollBar::add-line:horizontal {
   border: 2px transparent grey;
   border-top-right-radius: 7px;
   border-bottom-right-radius: 7px;
   background: rgba(34, 142, 255, 255);
   width: 20px;
   subcontrol-position: right;
   subcontrol-origin: margin;
}
QScrollBar::add-line:horizontal:pressed {
   border: 2px transparent grey;
   border-top-right-radius: 7px;
   border-bottom-right-radius: 7px;
   background: rgb(181,181,181);
   width: 20px;
   subcontrol-position: right;
   subcontrol-origin: margin;
}
QScrollBar::add-line:vertical {
   border: 2px transparent grey;
   border-bottom-left-radius: 7px;
   border-bottom-right-radius: 7px;
   background: rgba(34, 142, 255, 255);
   height: 20px;
   subcontrol-position: bottom;
   subcontrol-origin: margin;
}
QScrollBar::add-line:vertical:pressed {
   border: 2px transparent grey;
   border-bottom-left-radius: 7px;
   border-bottom-right-radius: 7px;
   background: rgb(181,181,181);
   height: 20px;
   subcontrol-position: bottom;
   subcontrol-origin: margin;
}
QScrollBar::sub-line:horizontal {
   border: 2px transparent grey;
   border-top-left-radius: 7px;
   border-bottom-left-radius: 7px;
   background: rgba(34, 142, 255, 255);
   width: 20px;
   subcontrol-position: left;
   subcontrol-origin: margin;
}
QScrollBar::sub-line:horizontal:pressed {
   border: 2px transparent grey;
   border-top-left-radius: 7px;
   border-bottom-left-radius: 7px;
   background: rgb(181,181,181);
   width: 20px;
   subcontrol-position: left;
   subcontrol-origin: margin;
}
QScrollBar::sub-line:vertical {
   border: 2px transparent grey;
   border-top-left-radius: 7px;
   border-top-right-radius: 7px;
   background: rgba(34, 142, 255, 255);
   height: 20px;
   subcontrol-position: top;
   subcontrol-origin: margin;
}
QScrollBar::sub-line:vertical:pressed {
   border: 2px transparent grey;
   border-top-left-radius: 7px;
   border-top-right-radius: 7px;
   background: rgb(181,181,181);
   height: 20px;
   subcontrol-position: top;
   subcontrol-origin: margin;
}
QScrollBar::left-arrow:horizontal {
   border: 1px transparent grey;
   border-top-left-radius: 3px;
   border-bottom-left-radius: 3px;
   width: 6px;
   height: 6px;
   background: white;
}
QScrollBar::right-arrow:horizontal {
   border: 1px transparent grey;
   border-top-right-radius: 3px;
   border-bottom-right-radius: 3px;
   width: 6px;
   height: 6px;
   background: white;
}
QScrollBar::up-arrow:vertical {
   border: 1px transparent grey;
   border-top-left-radius: 3px;
   border-top-right-radius: 3px;
   width: 6px;
   height: 6px;
   background: white;
}
QScrollBar::down-arrow:vertical {
   border: 1px transparent grey;
   border-bottom-left-radius: 3px;
   border-bottom-right-radius: 3px;
   width: 6px;
   height: 6px;
   background: white;
}
QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
   background: none;
}
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
   background: none;
}"""

# * MacOS Style Sheet for QT Applications
 #* Author: Jaime A. Quiroga P.
 #* Company: GTRONICK
 #* Last updated: 25/12/2020, 23:10.
 #* Available at: https://github.com/GTRONICK/QSS/blob/master/MacOS.qss
 #*/
mac_os = """/*

QMainWindow {
    background-color:#ececec;
}
QPushButton, QToolButton, QCommandLinkButton{
    padding: 0 5px 0 5px;
    border-style: solid;
    border-top-color: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-right-color: qlineargradient(spread:pad, x1:1, y1:0, x2:0, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-bottom-color: qlineargradient(spread:pad, x1:0, y1:1, x2:0, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-left-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-width: 2px;
    border-radius: 8px;
    color: #616161;
    font-weight: bold;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #fbfdfd, stop:0.5 #ffffff, stop:1 #fbfdfd);
}
QPushButton::default, QToolButton::default, QCommandLinkButton::default{
    border: 2px solid transparent;
    color: #FFFFFF;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #84afe5, stop:1 #1168e4);
}
QPushButton:hover, QToolButton:hover, QCommandLinkButton:hover{
    color: #3d3d3d;
}
QPushButton:pressed, QToolButton:pressed, QCommandLinkButton:pressed{
    color: #aeaeae;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #ffffff, stop:0.5 #fbfdfd, stop:1 #ffffff);
}
QPushButton:disabled, QToolButton:disabled, QCommandLinkButton:disabled{
    color: #616161;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #dce7eb, stop:0.5 #e0e8eb, stop:1 #dee7ec);
}
QLineEdit, QTextEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox, QTimeEdit, QDateEdit, QDateTimeEdit {
    border-width: 2px;
    border-radius: 8px;
    border-style: solid;
    border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-right-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-left-color: qlineargradient(spread:pad, x1:1, y1:0, x2:0, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    background-color: #f4f4f4;
    color: #3d3d3d;
}
QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QTimeEdit:focus, QDateEdit:focus, QDateTimeEdit:focus {
    border-width: 2px;
    border-radius: 8px;
    border-style: solid;
    border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 #85b7e3, stop:1 #9ec1db);
    border-right-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #85b7e3, stop:1 #9ec1db);
    border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #85b7e3, stop:1 #9ec1db);
    border-left-color: qlineargradient(spread:pad, x1:1, y1:0, x2:0, y2:0, stop:0 #85b7e3, stop:1 #9ec1db);
    background-color: #f4f4f4;
    color: #3d3d3d;
}
QLineEdit:disabled, QTextEdit:disabled, QPlainTextEdit:disabled, QSpinBox:disabled, QDoubleSpinBox:disabled, QTimeEdit:disabled, QDateEdit:disabled, QDateTimeEdit:disabled {
    color: #b9b9b9;
}
QSpinBox::up-button, QDoubleSpinBox::up-button, QTimeEdit::up-button, QDateEdit::up-button, QDateTimeEdit::up-button {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 15px;
    color: #272727;
    border-left-width: 1px;
    border-left-color: darkgray;
    border-left-style: solid;
    border-top-right-radius: 3px;
    padding: 3px;
}
QSpinBox::down-button, QDoubleSpinBox::down-button, QTimeEdit::down-button, QDateEdit::down-button, QDateTimeEdit::down-button {
    subcontrol-origin: padding;
    subcontrol-position: bottom right;
    width: 15px;
    color: #272727;
    border-left-width: 1px;
    border-left-color: darkgray;
    border-left-style: solid;
    border-bottom-right-radius: 3px;
    padding: 3px;
}
QSpinBox::up-button:pressed, QDoubleSpinBox::up-button:pressed, QTimeEdit::up-button:pressed, QDateEdit::up-button:pressed, QDateTimeEdit::up-button:pressed {
    color: #aeaeae;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #ffffff, stop:0.5 #fbfdfd, stop:1 #ffffff);
}
QSpinBox::down-button:pressed, QDoubleSpinBox::down-button:pressed, QTimeEdit::down-button:pressed, QDateEdit::down-button:pressed, QDateTimeEdit::down-button:pressed {
    color: #aeaeae;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #ffffff, stop:0.5 #fbfdfd, stop:1 #ffffff);
}
QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover, QTimeEdit::up-button:hover, QDateEdit::up-button:hover, QDateTimeEdit::up-button:hover {
    color: #FFFFFF;
    border-top-right-radius: 5px;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #84afe5, stop:1 #1168e4);
    
}
QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover, QTimeEdit::down-button:hover, QDateEdit::down-button:hover, QDateTimeEdit::down-button:hover {
    color: #FFFFFF;
    border-bottom-right-radius: 5px;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #84afe5, stop:1 #1168e4);
}
QSpinBox::up-arrow, QDoubleSpinBox::up-arrow, QTimeEdit::up-arrow, QDateEdit::up-arrow, QDateTimeEdit::up-arrow {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-up-symbolic.symbolic.png);
}
QSpinBox::down-arrow, QDoubleSpinBox::down-arrow, QTimeEdit::down-arrow, QDateEdit::down-arrow, QDateTimeEdit::down-arrow {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-down-symbolic.symbolic.png);
}
QProgressBar {
    max-height: 8px;
    text-align: center;
    font: italic bold 11px;
    color: #3d3d3d;
    border: 1px solid transparent;
    border-radius:4px;
    background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #ddd5d5, stop:0.5 #dad3d3, stop:1 #ddd5d5);
}
QProgressBar::chunk {
    background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #467dd1, stop:0.5 #3b88fc, stop:1 #467dd1);
    border-radius: 4px;
}
QProgressBar:disabled {
    color: #616161;
}
QProgressBar::chunk:disabled {
    background-color: #aeaeae;
}
QSlider::groove {
    border: 1px solid #bbbbbb;
    background-color: #52595d;
    border-radius: 4px;
}
QSlider::groove:horizontal {
    height: 6px;
}
QSlider::groove:vertical {
    width: 6px;
}
QSlider::handle:horizontal {
    background: #ffffff;
    border-style: solid;
    border-width: 1px;
    border-color: rgb(207,207,207);
    width: 12px;
    margin: -5px 0;
    border-radius: 7px;
}
QSlider::handle:vertical {
    background: #ffffff;
    border-style: solid;
    border-width: 1px;
    border-color: rgb(207,207,207);
    height: 12px;
    margin: 0 -5px;
    border-radius: 7px;
}
QSlider::add-page, QSlider::sub-page {
    border: 1px transparent;
    background-color: #52595d;
    border-radius: 4px;
}
QSlider::add-page:horizontal {
    background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #ddd5d5, stop:0.5 #dad3d3, stop:1 #ddd5d5);
}
QSlider::sub-page:horizontal {
    background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #467dd1, stop:0.5 #3b88fc, stop:1 #467dd1);
}
QSlider::add-page:vertical {
    background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #467dd1, stop:0.5 #3b88fc, stop:1 #467dd1);
}
QSlider::sub-page:vertical {
    background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #ddd5d5, stop:0.5 #dad3d3, stop:1 #ddd5d5);
}
QSlider::add-page:horizontal:disabled, QSlider::sub-page:horizontal:disabled, QSlider::add-page:vertical:disabled, QSlider::sub-page:vertical:disabled {
    background: #b9b9b9;
}
QComboBox, QFontComboBox {
    border-width: 2px;
    border-radius: 8px;
    border-style: solid;
    border-top-color: qlineargradient(spread:pad, x1:0.5, y1:1, x2:0.5, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-right-color: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-bottom-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #c1c9cf, stop:1 #d2d8dd);
    border-left-color: qlineargradient(spread:pad, x1:1, y1:0, x2:0, y2:0, stop:0 #c1c9cf, stop:1 #d2d8dd);
    background-color: #f4f4f4;
    color: #272727;
    padding-left: 5px;
}
QComboBox:editable, QComboBox:!editable, QComboBox::drop-down:editable, QComboBox:!editable:on, QComboBox::drop-down:editable:on {
    background: #ffffff;
}
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 15px;
    color: #272727;
    border-left-width: 1px;
    border-left-color: darkgray;
    border-left-style: solid;
    border-top-right-radius: 3px;
    border-bottom-right-radius: 3px;
}
QComboBox::down-arrow {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-down-symbolic.symbolic.png); /*Adawaita icon thene*/
}

QComboBox::down-arrow:on {
    top: 1px;
    left: 1px;
}
QComboBox QAbstractItemView {
    border: 1px solid darkgray;
    border-radius: 8px;
    selection-background-color: #dadada;
    selection-color: #272727;
    color: #272727;
    background: white;
}
QLabel, QCheckBox, QRadioButton {
    color: #272727;
}
QCheckBox {
    padding: 2px;
}
QCheckBox:disabled, QRadioButton:disabled {
    color: #808086;
    padding: 2px;
}

QCheckBox:hover {
    border-radius:4px;
    border-style:solid;
    padding-left: 1px;
    padding-right: 1px;
    padding-bottom: 1px;
    padding-top: 1px;
    border-width:1px;
    border-color: transparent;
}
QCheckBox::indicator:checked {
    image: url(/usr/share/icons/Adwaita/16x16/actions/object-select-symbolic.symbolic.png);
    height: 15px;
    width: 15px;
    border-style:solid;
    border-width: 1px;
    border-color: #48a5fd;
    color: #ffffff;
    border-radius: 3px;
    background-color: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #48a5fd, stop:0.5 #329cfb, stop:1 #48a5fd);
}
QCheckBox::indicator:unchecked {
    
    height: 15px;
    width: 15px;
    border-style:solid;
    border-width: 1px;
    border-color: #48a5fd;
    border-radius: 3px;
    background-color: #fbfdfa;
}
QLCDNumber {
    color: #616161;;
}
QMenuBar {
    background-color: #ececec;
}
QMenuBar::item {
    color: #616161;
    spacing: 3px;
    padding: 1px 4px;
    background-color: #ececec;
}

QMenuBar::item:selected {
    background-color: #dadada;
    color: #3d3d3d;
}
QMenu {
    background-color: #ececec;
}
QMenu::item:selected {
    background-color: #dadada;
    color: #3d3d3d;
}
QMenu::item {
    color: #616161;;
    background-color: #e0e0e0;
}
QTabWidget {
    color:rgb(0,0,0);
    background-color:#000000;
}
QTabWidget::pane {
    border-color: #050a0e;
    background-color: #e0e0e0;
    border-width: 1px;
    border-radius: 4px;
    position: absolute;
    top: -0.5em;
    padding-top: 0.5em;
}

QTabWidget::tab-bar {
    alignment: center;
}

QTabBar::tab {
    border-bottom: 1px solid #c0c0c0;
    padding: 3px;
    color: #272727;
    background-color: #fefefc;
    margin-left:0px;
}
QTabBar::tab:!last {
    border-right: 1px solid;
    border-right-color: #c0c0c0;
    border-bottom-color: #c0c0c0;
}
QTabBar::tab:first {
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
}
QTabBar::tab:last {
    border-top-right-radius: 4px;
    border-bottom-right-radius: 4px;
}
QTabBar::tab:selected, QTabBar::tab:last:selected, QTabBar::tab:hover {
    color: #FFFFFF;
    background-color: qlineargradient(spread:pad, x1:0.5, y1:0, x2:0.5, y2:1, stop:0 #84afe5, stop:1 #1168e4);
}
QRadioButton::indicator {
    height: 14px;
    width: 14px;
    border-style:solid;
    border-radius:7px;
    border-width: 1px;
}
QRadioButton::indicator:checked {
    border-color: #48a5fd;
    background-color: qradialgradient(cx:0.5, cy:0.5, radius:0.4,fx:0.5, fy:0.5, stop:0 #ffffff, stop:0.5 #ffffff, stop:0.6 #48a5fd, stop:1 #48a5fd);
}
QRadioButton::indicator:!checked {
    border-color: #a9b7c6;
    background-color: #fbfdfa;
}
QStatusBar {
    color:#027f7f;
}

QDial {
    background: #16a085;
}

QToolBox {
    color: #a9b7c6;
    background-color: #222b2e;
}
QToolBox::tab {
    color: #a9b7c6;
    background-color:#222b2e;
}
QToolBox::tab:selected {
    color: #FFFFFF;
    background-color:#222b2e;
}
QScrollArea {
    color: #FFFFFF;
    background-color:#222b2e;
}

QScrollBar:horizontal {
	max-height: 10px;
	border: 1px transparent grey;
	margin: 0px 20px 0px 20px;
	background: transparent;
}
QScrollBar:vertical {
	max-width: 10px;
	border: 1px transparent grey;
	margin: 20px 0px 20px 0px;
	background: transparent;
}
QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
	background: #52595d;
	border-style: transparent;
	border-radius: 4px;
	min-height: 25px;
}
QScrollBar::handle:horizontal:hover, QScrollBar::handle:vertical:hover {
	background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #467dd1, stop:0.5 #3b88fc, stop:1 #467dd1);
}
QScrollBar::add-line, QScrollBar::sub-line {
    border: 2px transparent grey;
    border-radius: 4px;
    subcontrol-origin: margin;
    background: #b9b9b9;
}
QScrollBar::add-line:horizontal {
    width: 20px;
    subcontrol-position: right;
}
QScrollBar::add-line:vertical {
    height: 20px;
    subcontrol-position: bottom;
}
QScrollBar::sub-line:horizontal {
    width: 20px;
    subcontrol-position: left;
}
QScrollBar::sub-line:vertical {
    height: 20px;
    subcontrol-position: top;
}
QScrollBar::add-line:vertical:pressed, QScrollBar::add-line:horizontal:pressed, QScrollBar::sub-line:horizontal:pressed, QScrollBar::sub-line:vertical:pressed {
    background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #467dd1, stop:0.5 #3b88fc, stop:1 #467dd1);
}
QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal, QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
    background: none;
}
QScrollBar::up-arrow:vertical {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-up-symbolic.symbolic.png);
}
QScrollBar::down-arrow:vertical {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-down-symbolic.symbolic.png);
}
QScrollBar::left-arrow:horizontal {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-previous-symbolic.symbolic.png);
}
QScrollBar::right-arrow:horizontal {
    image: url(/usr/share/icons/Adwaita/16x16/actions/go-next-symbolic.symbolic.png);
}"""
