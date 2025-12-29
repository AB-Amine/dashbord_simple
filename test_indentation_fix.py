#!/usr/bin/env python3
"""
Test script to verify the indentation fix in Spark streaming main file.
"""

import sys
import os
import ast

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_syntax():
    """Test that the Python syntax is correct."""
    try:
        with open('src/analytics/spark_streaming_main.py', 'r') as f:
            code = f.read()
        
        # Parse the code to check for syntax errors
        ast.parse(code)
        print("✅ Python syntax is correct")
        return True
        
    except SyntaxError as e:
        print(f"❌ Syntax error found: {e}")
        return False
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False

def test_indentation_fix():
    """Test that the specific indentation issue is fixed."""
    try:
        with open('src/analytics/spark_streaming_main.py', 'r') as f:
            lines = f.readlines()
        
        # Check the specific lines that had the indentation error
        for i, line in enumerate(lines[354:358], start=355):
            print(f"Line {i}: {repr(line)}")
            
            # Check that line 356 (index 355) has proper indentation
            if i == 356:
                if not line.startswith('                        .drop'):
                    print(f"❌ Line {i} has incorrect indentation")
                    return False
                if not line.rstrip().endswith('\\'):
                    print(f"❌ Line {i} is missing continuation character")
                    return False
            
            # Check that line 357 (index 356) has proper indentation  
            elif i == 357:
                if not line.startswith('                        .withColumn'):
                    print(f"❌ Line {i} has incorrect indentation")
                    return False
        
        print("✅ Indentation fix verified")
        return True
        
    except Exception as e:
        print(f"❌ Error checking indentation: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing indentation fix...")
    
    success = True
    
    if not test_syntax():
        success = False
    
    if not test_indentation_fix():
        success = False
    
    if success:
        print("🎉 All tests passed! Indentation error is fixed.")
    else:
        print("❌ Some tests failed.")
    
    sys.exit(0 if success else 1)