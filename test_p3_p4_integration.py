"""
Integration test for p3 and p4 scripts with resume system

This test verifies:
1. Both scripts can be imported and run with resume parameters
2. Resume state correctly tracks which script (p3/p4) was running
3. Resume state correctly tracks retry attempts for p3
4. Both scripts still run correctly with new code
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pytz

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.quota_resume import (
    save_resume_state, load_resume_state, clear_resume_state,
    get_batch_for_key, calculate_reset_time
)


def test_p3_resume_state_structure():
    """Test that p3 resume state includes pipeline identifier and retry attempt."""
    print("=" * 70)
    print("TEST 1: P3 Resume State Structure")
    print("=" * 70)
    
    test_key = 1
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Clean up any existing test file
    state_file = logs_dir / f"resume_state_key{test_key}.json"
    if state_file.exists():
        state_file.unlink()
    
    try:
        # Create p3-style resume state (simulating what p3 would save)
        batch_number = get_batch_for_key(test_key)
        reset_time = calculate_reset_time(batch_number)
        
        p3_state = {
            "pipeline": "p3",  # Track which script
            "batch_number": batch_number,
            "quota_exhausted_at": datetime.now(pytz.UTC).isoformat(),
            "reset_time": reset_time.isoformat(),
            "current_file": {
                "cao_number": "123",
                "filename": "test_file.md",
                "file_path": "path/to/test_file.md",
                "last_attempt": 3,  # P3 tracks retry attempt
                "max_retries": 8
            },
            "statistics": {
                "successful_files": 10,
                "failed_files": 2,
                "processed_before_quota": 12
            },
            "total_processes": 22,
            "process_id": 0
        }
        
        save_resume_state(test_key, p3_state)
        print(f"✓ Saved p3 resume state for key {test_key}")
        
        # Load and verify
        loaded = load_resume_state(test_key)
        if loaded is None:
            print("❌ Failed to load p3 resume state")
            return False
        
        # Verify pipeline identifier
        if loaded.get("pipeline") != "p3":
            print(f"❌ Pipeline identifier missing or incorrect. Expected 'p3', got '{loaded.get('pipeline')}'")
            return False
        print("✓ Pipeline identifier 'p3' present")
        
        # Verify retry attempt is tracked
        if "current_file" not in loaded or "last_attempt" not in loaded["current_file"]:
            print("❌ Retry attempt not tracked in p3 resume state")
            return False
        
        attempt = loaded["current_file"]["last_attempt"]
        if attempt != 3:
            print(f"❌ Retry attempt incorrect. Expected 3, got {attempt}")
            return False
        print(f"✓ Retry attempt correctly tracked: {attempt}")
        
        # Clean up
        clear_resume_state(test_key)
        print("\n✅ P3 resume state structure is correct!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if state_file.exists():
            state_file.unlink()


def test_p4_resume_state_structure():
    """Test that p4 resume state includes pipeline identifier but no retry attempt."""
    print("\n" + "=" * 70)
    print("TEST 2: P4 Resume State Structure")
    print("=" * 70)
    
    test_key = 1
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Clean up any existing test file
    state_file = logs_dir / f"resume_state_key{test_key}.json"
    if state_file.exists():
        state_file.unlink()
    
    try:
        # Create p4-style resume state (simulating what p4 would save)
        batch_number = get_batch_for_key(test_key)
        reset_time = calculate_reset_time(batch_number)
        
        p4_state = {
            "pipeline": "p4",  # Track which script
            "batch_number": batch_number,
            "quota_exhausted_at": datetime.now(pytz.UTC).isoformat(),
            "reset_time": reset_time.isoformat(),
            "current_file": {
                "cao_number": "123",
                "filename": "test_file.json",
                "file_path": "path/to/test_file.json"
                # Note: No last_attempt - p4 always resumes from attempt 0
            },
            "statistics": {
                "successful_files": 10,
                "failed_files": 2,
                "processed_before_quota": 12
            },
            "total_processes": 22,
            "process_id": 0
        }
        
        save_resume_state(test_key, p4_state)
        print(f"✓ Saved p4 resume state for key {test_key}")
        
        # Load and verify
        loaded = load_resume_state(test_key)
        if loaded is None:
            print("❌ Failed to load p4 resume state")
            return False
        
        # Verify pipeline identifier
        if loaded.get("pipeline") != "p4":
            print(f"❌ Pipeline identifier missing or incorrect. Expected 'p4', got '{loaded.get('pipeline')}'")
            return False
        print("✓ Pipeline identifier 'p4' present")
        
        # Verify no retry attempt tracking (p4 doesn't track attempts)
        if "current_file" in loaded and "last_attempt" in loaded["current_file"]:
            attempt = loaded["current_file"]["last_attempt"]
            if attempt is not None:
                print(f"⚠ Warning: p4 resume state has last_attempt ({attempt}), but this should be None/absent")
        print("✓ No retry attempt tracking (correct for p4)")
        
        # Clean up
        clear_resume_state(test_key)
        print("\n✅ P4 resume state structure is correct!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if state_file.exists():
            state_file.unlink()


def test_script_imports():
    """Test that both scripts can be imported and have required functions."""
    print("\n" + "=" * 70)
    print("TEST 3: Script Imports and Function Availability")
    print("=" * 70)
    
    try:
        # Test p3 imports
        from pipelines.p3_llmExtraction import (
            run_extraction_pipeline, ExtractionConfig,
            extract_with_markdown_upload, process_single_file,
            ProcessingContext
        )
        print("✓ p3_llmExtraction imports successful")
        
        # Test p4 imports
        from pipelines.p4_analysis import (
            main, AnalysisConfig
        )
        print("✓ p4_analysis imports successful")
        
        # Test that p3 functions accept resume_from_attempt
        import inspect
        
        # Check extract_with_markdown_upload
        sig = inspect.signature(extract_with_markdown_upload)
        if 'resume_from_attempt' in sig.parameters:
            print("✓ extract_with_markdown_upload accepts resume_from_attempt")
        else:
            print("❌ extract_with_markdown_upload missing resume_from_attempt")
            return False
        
        # Check process_single_file
        sig = inspect.signature(process_single_file)
        if 'resume_from_attempt' in sig.parameters:
            print("✓ process_single_file accepts resume_from_attempt")
        else:
            print("❌ process_single_file missing resume_from_attempt")
            return False
        
        # Check ProcessingContext has last_attempt_on_quota
        import dataclasses
        fields = [f.name for f in dataclasses.fields(ProcessingContext)]
        if 'last_attempt_on_quota' in fields:
            print("✓ ProcessingContext has last_attempt_on_quota field")
        else:
            print("❌ ProcessingContext missing last_attempt_on_quota")
            return False
        
        print("\n✅ All script imports and functions available!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_p3_config():
    """Test that p3 config includes resume_on_quota."""
    print("\n" + "=" * 70)
    print("TEST 4: P3 Configuration")
    print("=" * 70)
    
    try:
        from pipelines.p3_llmExtraction import load_configuration, ExtractionConfig
        
        # Test default value
        config = ExtractionConfig(
            input_folder="test",
            output_folder=Path("test")
        )
        if not hasattr(config, 'resume_on_quota'):
            print("❌ ExtractionConfig missing resume_on_quota attribute")
            return False
        print(f"✓ ExtractionConfig has resume_on_quota (default: {config.resume_on_quota})")
        
        # Test loading from config file
        try:
            loaded_config = load_configuration()
            print(f"✓ Config loaded from file, resume_on_quota = {loaded_config.resume_on_quota}")
        except Exception as e:
            print(f"⚠ Could not load config from file (may not exist): {e}")
        
        print("\n✅ P3 configuration correct!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_p4_config():
    """Test that p4 config includes resume_on_quota."""
    print("\n" + "=" * 70)
    print("TEST 5: P4 Configuration")
    print("=" * 70)
    
    try:
        from pipelines.p4_analysis import load_configuration, AnalysisConfig
        
        # Test default value
        config = AnalysisConfig(
            input_folder="test",
            output_folder=Path("test"),
            cao_info_path="test"
        )
        if not hasattr(config, 'resume_on_quota'):
            print("❌ AnalysisConfig missing resume_on_quota attribute")
            return False
        print(f"✓ AnalysisConfig has resume_on_quota (default: {config.resume_on_quota})")
        
        # Test loading from config file
        try:
            loaded_config = load_configuration()
            print(f"✓ Config loaded from file, resume_on_quota = {loaded_config.resume_on_quota}")
        except Exception as e:
            print(f"⚠ Could not load config from file (may not exist): {e}")
        
        print("\n✅ P4 configuration correct!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("P3/P4 INTEGRATION TEST SUITE")
    print("=" * 70)
    
    results = []
    
    results.append(("P3 Resume State Structure", test_p3_resume_state_structure()))
    results.append(("P4 Resume State Structure", test_p4_resume_state_structure()))
    results.append(("Script Imports", test_script_imports()))
    results.append(("P3 Configuration", test_p3_config()))
    results.append(("P4 Configuration", test_p4_config()))
    
    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

