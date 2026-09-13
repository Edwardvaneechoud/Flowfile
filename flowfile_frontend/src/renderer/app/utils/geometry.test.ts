import { describe, it, expect } from "vitest";
import {
  describeGeometry,
  detectGeometryColumns,
  isGeoArrowDataType,
  isGeometryColumn,
  isWktValue,
  parseWkt,
} from "./geometry";

// Every string here is producible by the geospatial nodes: the polars path emits
// "4.0" and "1e-7", DuckDB's ST_AsText emits "4" and "1e-09" and flattens
// MULTIPOINT to a form with no inner parens, and a bad CRS yields "inf".
const GEOMETRY = [
  "POINT (4.9041 52.3676)",
  "POINT(4.9041 52.3676)",
  "point (4.9041 52.3676)",
  "POINT (4 52)",
  "POINT (4.0 52.0)",
  "POINT (1e-7 52.0)",
  "POINT (1e-09 52)",
  "POINT (inf inf)",
  "POINT (.5 52)",
  "POINT (-4.9 -52.3)",
  "POINT Z (1 2 3)",
  "POINTZ (1 2 3)",
  "POINT ZM (1 2 3 4)",
  "POINT EMPTY",
  "point empty",
  "POLYGON EMPTY",
  "SRID=4326;POINT (4.9041 52.3676)",
  "LINESTRING (4.9041 52.3676, 4.91 52.37)",
  "POLYGON ((0 0, 1 0, 1 1, 0 0))",
  "MULTIPOINT (1 2, 3 4)",
  "MULTIPOINT ((1 2), (3 4))",
  "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
  "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
  "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
  "  POINT (4.9 52.3)  ",
  "POLYGON ((0 0,\n1 0,\n1 1,\n0 0))",
];

// The false positives that matter. A permissive prefix match — which is what
// kepler.gl actually ships — accepts most of these.
const NOT_GEOMETRY = [
  "Laser POINT (red) 5mW handheld presenter",
  "MULTIPOINT (4-pin) connector harness",
  "point (see appendix)",
  "POINT (4.9041 52.3676) is Amsterdam",
  "POINT (1 2)garbage",
  "LINESTRING (0 0, 1 1) route A to B",
  "SELECT ST_AsText(geom) FROM t",
  "POINT(3)",
  "=POINT(A1)",
  "POINTLESS (1 2)",
  "Amsterdam",
  "",
  "12345",
  "{\"type\":\"Point\",\"coordinates\":[4.9,52.3]}",
  "0101000000EE5A423EE8991340F1F44A5986304A40",
  "POLYGON ((0 0, 1 0, 1 1, 0 0)) -- the office",
  "the POINT (1 2)",
];

describe("parseWkt", () => {
  it("accepts every form the geospatial nodes can produce", () => {
    for (const value of GEOMETRY) {
      expect(parseWkt(value), `should parse: ${value}`).not.toBeNull();
    }
  });

  it("rejects free text that merely opens like geometry", () => {
    for (const value of NOT_GEOMETRY) {
      expect(parseWkt(value), `should reject: ${value}`).toBeNull();
    }
  });

  it("rejects trailing prose that a real WKT parser would accept", () => {
    // DuckDB's ST_GeomFromText returns a valid POINT for this input.
    expect(parseWkt("POINT (4.9041 52.3676) is Amsterdam")).toBeNull();
  });

  it("reports type, dimension and vertex count", () => {
    expect(parseWkt("POLYGON ((0 0, 1 0, 1 1, 0 0))")).toMatchObject({
      type: "Polygon",
      dimension: "",
      points: 4,
    });
    expect(parseWkt("POINT Z (1 2 3)")).toMatchObject({ type: "Point", dimension: "Z", points: 1 });
    expect(parseWkt("POINT EMPTY")).toMatchObject({ type: "Point", points: 0, coordinate: null });
    expect(parseWkt("MULTIPOINT (1 2, 3 4)")?.points).toBe(2);
  });

  it("extracts the coordinate of a single point", () => {
    expect(parseWkt("POINT (4.9041 52.3676)")?.coordinate).toEqual([4.9041, 52.3676]);
    expect(parseWkt("SRID=4326;POINT (4.9041 52.3676)")?.coordinate).toEqual([4.9041, 52.3676]);
  });

  it("ignores non-strings and absurd lengths without stalling", () => {
    expect(isWktValue(null)).toBe(false);
    expect(isWktValue(42)).toBe(false);
    expect(isWktValue({ type: "Point" })).toBe(false);
    const start = Date.now();
    expect(isWktValue("(".repeat(5000))).toBe(false);
    expect(Date.now() - start).toBeLessThan(1000);
  });
});

describe("isGeometryColumn", () => {
  it("detects a column of geometry", () => {
    expect(isGeometryColumn(["POINT (1 2)", "POINT (3 4)"])).toBe(true);
  });

  it("tolerates nulls and sentinel blanks, including an all-null head", () => {
    expect(isGeometryColumn([null, null, "POINT (1 2)"])).toBe(true);
    expect(isGeometryColumn(["POINT (1 2)", "", "N/A", "-", null])).toBe(true);
  });

  it("refuses a column with no evidence", () => {
    expect(isGeometryColumn([])).toBe(false);
    expect(isGeometryColumn([null, null])).toBe(false);
    expect(isGeometryColumn(["", "N/A"])).toBe(false);
  });

  it("refuses a text column containing a single geometry-looking row", () => {
    const notes = ["meeting notes", "POINT (1 2)", "follow up"];
    expect(isGeometryColumn(notes)).toBe(false);
  });

  it("refuses a mixed column rather than guessing", () => {
    expect(isGeometryColumn([...Array(99).fill("POINT (1 2)"), "see appendix"])).toBe(false);
  });
});

describe("isGeoArrowDataType", () => {
  it("recognises the dtype polars surfaces for a GeoArrow extension field", () => {
    expect(isGeoArrowDataType("Extension('geoarrow.wkb', Binary, '{}')")).toBe(true);
    expect(isGeoArrowDataType("String")).toBe(false);
    expect(isGeoArrowDataType(undefined)).toBe(false);
  });
});

describe("describeGeometry", () => {
  it("shows the coordinate for a single point", () => {
    expect(describeGeometry("POINT (4.9041 52.3676)")).toBe("◆ Point (4.9041, 52.3676)");
  });

  it("shows shape and vertex count for everything else", () => {
    expect(describeGeometry("POLYGON ((0 0, 1 0, 1 1, 0 0))")).toBe("◆ Polygon · 4 pts");
    expect(describeGeometry("MULTIPOINT (1 2, 3 4)")).toBe("◆ MultiPoint · 2 pts");
  });

  it("marks empty geometry and carries the dimension tag", () => {
    expect(describeGeometry("POLYGON EMPTY")).toBe("◆ Polygon (empty)");
    expect(describeGeometry("POINT Z (1 2 3)")).toBe("◆ Point Z (1, 2)");
  });

  it("returns null for a non-geometry value so the caller can fall back", () => {
    expect(describeGeometry("Amsterdam")).toBeNull();
    expect(describeGeometry(null)).toBeNull();
  });
});

describe("detectGeometryColumns", () => {
  // The names the geospatial nodes actually produce — "geometry" is only one of them.
  const schema = [
    { name: "district", data_type: "String", data_type_group: "String" },
    { name: "residents", data_type: "Int64", data_type_group: "Numeric" },
    { name: "geometry", data_type: "String", data_type_group: "String" },
    { name: "zone", data_type: "String", data_type_group: "String" },
  ];
  const rows = [
    {
      district: "Centrum",
      residents: 86000,
      geometry: "POLYGON ((4.88 52.36, 4.92 52.36, 4.92 52.39, 4.88 52.36))",
      zone: "POINT (4.9041 52.3676)",
    },
  ];

  it("finds every geometry column regardless of its name", () => {
    expect(detectGeometryColumns(schema, rows)).toEqual(new Set(["geometry", "zone"]));
  });

  it("leaves ordinary text and numeric columns alone", () => {
    const found = detectGeometryColumns(schema, rows);
    expect(found.has("district")).toBe(false);
    expect(found.has("residents")).toBe(false);
  });

  it("never sniffs a non-string column", () => {
    const binary = [{ name: "g", data_type: "Binary", data_type_group: "Binary" }];
    expect(detectGeometryColumns(binary, [{ g: "POINT (1 2)" }]).size).toBe(0);
  });

  it("trusts a declared GeoArrow dtype without sniffing", () => {
    const geoarrow = [
      { name: "g", data_type: "Extension('geoarrow.wkb', Binary, '{}')", data_type_group: "Binary" },
    ];
    expect(detectGeometryColumns(geoarrow, [])).toEqual(new Set(["g"]));
  });

  it("returns nothing without a schema or rows", () => {
    expect(detectGeometryColumns(null, rows).size).toBe(0);
    expect(detectGeometryColumns(schema, null).size).toBe(0);
    expect(detectGeometryColumns(schema, []).size).toBe(0);
  });
});
