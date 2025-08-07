#!/usr/bin/env python3
"""
SQL Library CLI Tool

This CLI provides access to the SQL library functionality including
patterns, data quality checks, functions, and templates.
"""

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, Any, List

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sql_library.core.sql_patterns import SQLPatterns
from sql_library.core.data_quality import DataQualityChecks
from sql_library.core.sql_functions import SQLFunctions
from sql_library.core.sql_templates import SQLTemplates


def list_patterns(args):
    """List available SQL patterns."""
    patterns = SQLPatterns()

    if args.category:
        pattern_list = patterns.list_patterns(category=args.category)
        print(f"📊 SQL Patterns - Category: {args.category}")
    else:
        pattern_list = patterns.list_patterns()
        print("📊 All SQL Patterns")

    print("=" * 50)

    for pattern in pattern_list:
        print(f"🔹 {pattern.name}")
        print(f"   Description: {pattern.description}")
        print(f"   Category: {pattern.category}")
        print(f"   Parameters: {', '.join(pattern.parameters)}")
        print(f"   Tags: {', '.join(pattern.tags)}")
        print()


def render_pattern(args):
    """Render a SQL pattern with parameters."""
    patterns = SQLPatterns()

    try:
        # Parse parameters from JSON file or command line
        if args.parameters_file:
            with open(args.parameters_file, "r") as f:
                parameters = json.load(f)
        else:
            parameters = {}
            if args.parameters:
                for param in args.parameters:
                    key, value = param.split("=", 1)
                    parameters[key] = value

        sql = patterns.render_pattern(args.pattern_name, parameters)

        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(sql)
            print(f"✅ SQL pattern rendered and saved to {args.output_file}")
        else:
            print("📝 Generated SQL:")
            print("=" * 50)
            print(sql)

    except Exception as e:
        print(f"❌ Error rendering pattern: {e}")


def list_quality_checks(args):
    """List available data quality checks."""
    quality = DataQualityChecks()

    if args.category:
        check_list = quality.list_checks(category=args.category)
        print(f"📊 Data Quality Checks - Category: {args.category}")
    elif args.severity:
        check_list = quality.list_checks(severity=args.severity)
        print(f"📊 Data Quality Checks - Severity: {args.severity}")
    else:
        check_list = quality.list_checks()
        print("📊 All Data Quality Checks")

    print("=" * 50)

    for check in check_list:
        print(f"🔹 {check.name}")
        print(f"   Description: {check.description}")
        print(f"   Category: {check.category}")
        print(f"   Severity: {check.severity}")
        print(f"   Parameters: {', '.join(check.parameters)}")
        print(f"   Expected: {check.expected_result}")
        print()


def render_quality_check(args):
    """Render a data quality check with parameters."""
    quality = DataQualityChecks()

    try:
        # Parse parameters from JSON file or command line
        if args.parameters_file:
            with open(args.parameters_file, "r") as f:
                parameters = json.load(f)
        else:
            parameters = {}
            if args.parameters:
                for param in args.parameters:
                    key, value = param.split("=", 1)
                    parameters[key] = value

        sql = quality.render_check(args.check_name, parameters)

        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(sql)
            print(f"✅ Quality check rendered and saved to {args.output_file}")
        else:
            print("📝 Generated SQL:")
            print("=" * 50)
            print(sql)

    except Exception as e:
        print(f"❌ Error rendering quality check: {e}")


def list_functions(args):
    """List available SQL functions."""
    functions = SQLFunctions()

    if args.category:
        function_list = functions.list_functions(category=args.category)
        print(f"📊 SQL Functions - Category: {args.category}")
    else:
        function_list = functions.list_functions()
        print("📊 All SQL Functions")

    print("=" * 50)

    for function in function_list:
        print(f"🔹 {function.name}")
        print(f"   Description: {function.description}")
        print(f"   Category: {function.category}")
        print(f"   Return Type: {function.return_type}")
        print(f"   Parameters: {', '.join(function.parameters)}")
        print(f"   Tags: {', '.join(function.tags)}")
        print()


def render_function(args):
    """Render a SQL function definition."""
    functions = SQLFunctions()

    try:
        sql = functions.render_function_definition(args.function_name)

        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(sql)
            print(f"✅ Function definition saved to {args.output_file}")
        else:
            print("📝 Generated SQL:")
            print("=" * 50)
            print(sql)

    except Exception as e:
        print(f"❌ Error rendering function: {e}")


def create_function_library(args):
    """Create a complete SQL function library."""
    functions = SQLFunctions()

    try:
        library_sql = functions.create_function_library(args.output_file)

        if args.output_file:
            print(f"✅ Function library created: {args.output_file}")
        else:
            print("📝 Generated Function Library:")
            print("=" * 50)
            print(
                library_sql[:1000] + "..." if len(library_sql) > 1000 else library_sql
            )

    except Exception as e:
        print(f"❌ Error creating function library: {e}")


def list_templates(args):
    """List available SQL templates."""
    templates = SQLTemplates()

    if args.category:
        template_list = templates.list_templates(category=args.category)
        print(f"📊 SQL Templates - Category: {args.category}")
    else:
        template_list = templates.list_templates()
        print("📊 All SQL Templates")

    print("=" * 50)

    for template in template_list:
        print(f"🔹 {template.name}")
        print(f"   Description: {template.description}")
        print(f"   Category: {template.category}")
        print(f"   Parameters: {', '.join(template.parameters)}")
        print(f"   Tags: {', '.join(template.tags)}")
        print()


def render_template(args):
    """Render a SQL template with parameters."""
    templates = SQLTemplates()

    try:
        # Parse parameters from JSON file or command line
        if args.parameters_file:
            with open(args.parameters_file, "r") as f:
                parameters = json.load(f)
        else:
            parameters = {}
            if args.parameters:
                for param in args.parameters:
                    key, value = param.split("=", 1)
                    parameters[key] = value

        sql = templates.render_template(args.template_name, parameters)

        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(sql)
            print(f"✅ SQL template rendered and saved to {args.output_file}")
        else:
            print("📝 Generated SQL:")
            print("=" * 50)
            print(sql)

    except Exception as e:
        print(f"❌ Error rendering template: {e}")


def create_template_library(args):
    """Create a complete SQL template library."""
    templates = SQLTemplates()

    try:
        library_sql = templates.create_template_library(args.output_file)

        if args.output_file:
            print(f"✅ Template library created: {args.output_file}")
        else:
            print("📝 Generated Template Library:")
            print("=" * 50)
            print(
                library_sql[:1000] + "..." if len(library_sql) > 1000 else library_sql
            )

    except Exception as e:
        print(f"❌ Error creating template library: {e}")


def search_library(args):
    """Search across all SQL library components."""
    print(f"🔍 Searching SQL Library for: '{args.query}'")
    print("=" * 50)

    # Search patterns
    patterns = SQLPatterns()
    pattern_results = patterns.search_patterns(args.query)
    if pattern_results:
        print("📊 SQL Patterns:")
        for pattern in pattern_results:
            print(f"   • {pattern.name}: {pattern.description}")
        print()

    # Search quality checks
    quality = DataQualityChecks()
    check_results = quality.search_checks(args.query)
    if check_results:
        print("🔍 Data Quality Checks:")
        for check in check_results:
            print(f"   • {check.name}: {check.description}")
        print()

    # Search functions
    functions = SQLFunctions()
    function_results = functions.search_functions(args.query)
    if function_results:
        print("⚙️  SQL Functions:")
        for function in function_results:
            print(f"   • {function.name}: {function.description}")
        print()

    # Search templates
    templates = SQLTemplates()
    template_results = templates.search_templates(args.query)
    if template_results:
        print("📋 SQL Templates:")
        for template in template_results:
            print(f"   • {template.name}: {template.description}")
        print()

    total_results = (
        len(pattern_results)
        + len(check_results)
        + len(function_results)
        + len(template_results)
    )
    print(f"📈 Total results found: {total_results}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="SQL Library CLI Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all SQL patterns
  python sql_library/cli/sql_library_cli.py list-patterns

  # Render a bronze ingestion pattern
  python sql_library/cli/sql_library_cli.py render-pattern bronze_ingestion --parameters catalog=hive_metastore schema=retail table_name=customers

  # List data quality checks
  python sql_library/cli/sql_library_cli.py list-quality-checks

  # Create function library
  python sql_library/cli/sql_library_cli.py create-function-library --output functions.sql

  # Search library
  python sql_library/cli/sql_library_cli.py search "bronze"
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Patterns subcommands
    patterns_parser = subparsers.add_parser("list-patterns", help="List SQL patterns")
    patterns_parser.add_argument("--category", help="Filter by category")
    patterns_parser.set_defaults(func=list_patterns)

    render_pattern_parser = subparsers.add_parser(
        "render-pattern", help="Render a SQL pattern"
    )
    render_pattern_parser.add_argument("pattern_name", help="Pattern name to render")
    render_pattern_parser.add_argument(
        "--parameters", nargs="+", help="Parameters as key=value pairs"
    )
    render_pattern_parser.add_argument(
        "--parameters-file", help="JSON file with parameters"
    )
    render_pattern_parser.add_argument(
        "--output-file", help="Output file for generated SQL"
    )
    render_pattern_parser.set_defaults(func=render_pattern)

    # Quality checks subcommands
    quality_parser = subparsers.add_parser(
        "list-quality-checks", help="List data quality checks"
    )
    quality_parser.add_argument("--category", help="Filter by category")
    quality_parser.add_argument("--severity", help="Filter by severity")
    quality_parser.set_defaults(func=list_quality_checks)

    render_check_parser = subparsers.add_parser(
        "render-quality-check", help="Render a data quality check"
    )
    render_check_parser.add_argument("check_name", help="Check name to render")
    render_check_parser.add_argument(
        "--parameters", nargs="+", help="Parameters as key=value pairs"
    )
    render_check_parser.add_argument(
        "--parameters-file", help="JSON file with parameters"
    )
    render_check_parser.add_argument(
        "--output-file", help="Output file for generated SQL"
    )
    render_check_parser.set_defaults(func=render_quality_check)

    # Functions subcommands
    functions_parser = subparsers.add_parser(
        "list-functions", help="List SQL functions"
    )
    functions_parser.add_argument("--category", help="Filter by category")
    functions_parser.set_defaults(func=list_functions)

    render_function_parser = subparsers.add_parser(
        "render-function", help="Render a SQL function"
    )
    render_function_parser.add_argument("function_name", help="Function name to render")
    render_function_parser.add_argument(
        "--output-file", help="Output file for generated SQL"
    )
    render_function_parser.set_defaults(func=render_function)

    create_library_parser = subparsers.add_parser(
        "create-function-library", help="Create function library"
    )
    create_library_parser.add_argument("--output-file", help="Output file for library")
    create_library_parser.set_defaults(func=create_function_library)

    # Templates subcommands
    templates_parser = subparsers.add_parser(
        "list-templates", help="List SQL templates"
    )
    templates_parser.add_argument("--category", help="Filter by category")
    templates_parser.set_defaults(func=list_templates)

    render_template_parser = subparsers.add_parser(
        "render-template", help="Render a SQL template"
    )
    render_template_parser.add_argument("template_name", help="Template name to render")
    render_template_parser.add_argument(
        "--parameters", nargs="+", help="Parameters as key=value pairs"
    )
    render_template_parser.add_argument(
        "--parameters-file", help="JSON file with parameters"
    )
    render_template_parser.add_argument(
        "--output-file", help="Output file for generated SQL"
    )
    render_template_parser.set_defaults(func=render_template)

    create_template_library_parser = subparsers.add_parser(
        "create-template-library", help="Create template library"
    )
    create_template_library_parser.add_argument(
        "--output-file", help="Output file for library"
    )
    create_template_library_parser.set_defaults(func=create_template_library)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search SQL library")
    search_parser.add_argument("query", help="Search query")
    search_parser.set_defaults(func=search_library)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        args.func(args)
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
