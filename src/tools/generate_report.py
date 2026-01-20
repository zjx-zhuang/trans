import os
import glob
import re

def parse_report_file(filepath):
    """Parses a single conversion report file."""
    filename = os.path.basename(filepath)
    
    # Infer SQL Type from filename
    if filename.startswith("ddl"):
        sql_type = "DDL"
    elif filename.startswith("dml"):
        sql_type = "DML"
    else:
        sql_type = "SQL" # Default/Other

    result = "Unknown"
    validation = "Unknown"
    retries = 0
    duration = "0s"
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Extract Duration
        duration_match = re.search(r'转换耗时:\s*(.+)', content)
        if duration_match:
            duration = duration_match.group(1).strip()

        # Extract Overall Result
        result_match = re.search(r'整体结果:\s*(.+)', content)
        if result_match:
            result = result_match.group(1).strip()
            
        # Extract Validation Result
        validation_match = re.search(r'BigQuery 验证:\s*(.+)', content)
        if validation_match:
            validation = validation_match.group(1).strip()

        # Extract Retry Count
        retry_match = re.search(r'重试次数:\s*(\d+)', content)
        if retry_match:
            retries = int(retry_match.group(1))

    return {
        "Filename": filename,
        "Type": sql_type,
        "Result": result,
        "Validation": validation,
        "Retries": retries,
        "Duration": duration
    }

def generate_report(result_dir):
    """Generates a statistical report from all files in result_dir."""
    files = glob.glob(os.path.join(result_dir, "*.txt"))
    data = []
    
    for filepath in sorted(files):
        data.append(parse_report_file(filepath))
        
    if not data:
        print("No report files found in", result_dir)
        return

    # 1. Summary Statistics
    total_files = len(data)
    success_count = sum(1 for d in data if "成功" in d["Result"] or "Success" in d["Result"])
    fail_count = total_files - success_count
    
    type_counts = {}
    for d in data:
        type_counts[d["Type"]] = type_counts.get(d["Type"], 0) + 1
        
    print("\n" + "="*80)
    print("📊 Conversion Statistical Report")
    print("="*80)
    print(f"Total Files: {total_files}")
    print(f"Success:     {success_count}")
    print(f"Failed:      {fail_count}")
    print("-" * 20)
    print("Count by Type:")
    for t, c in type_counts.items():
        print(f"  {t}: {c}")
    print("="*80 + "\n")

    # 2. Detailed Table
    headers = ["Filename", "Type", "Result", "Validation", "Retries", "Duration"]
    
    # Generate Markdown Content
    md_lines = []
    md_lines.append(f"# 📊 SQL Conversion Report")
    md_lines.append(f"**Generated:** {os.popen('date').read().strip()}")
    md_lines.append(f"")
    md_lines.append(f"## Summary")
    md_lines.append(f"- **Total Files:** {total_files}")
    md_lines.append(f"- **Success:** {success_count}")
    md_lines.append(f"- **Failed:** {fail_count}")
    md_lines.append(f"")
    md_lines.append(f"### Count by Type")
    for t, c in type_counts.items():
        md_lines.append(f"- **{t}:** {c}")
    md_lines.append(f"")
    md_lines.append(f"## Detailed Results")
    md_lines.append(f"| " + " | ".join(headers) + " |")
    md_lines.append(f"| " + " | ".join(["---"] * len(headers)) + " |")
    
    for row in data:
        md_lines.append(f"| " + " | ".join(str(row[h]) for h in headers) + " |")
    
    md_content = "\n".join(md_lines)
    
    # Write to file
    output_path = os.path.join(result_dir, "conversion_report.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md_content)
        
    print(f"Report generated at: {output_path}")

if __name__ == "__main__":
    # Assuming the script is run from project root or src/tools
    # Adjust path to find result folder
    
    # Try absolute path first
    result_dir = "/Users/apple/work/transv1/trans/result"
    
    if not os.path.exists(result_dir):
        # Fallback to relative path
        result_dir = "../../result" 
        
    if os.path.exists(result_dir):
        generate_report(result_dir)
    else:
        print(f"Error: Result directory not found at {result_dir}")
