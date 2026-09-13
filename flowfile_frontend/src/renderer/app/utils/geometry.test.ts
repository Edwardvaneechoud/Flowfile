import { describe, it, expect } from "vitest";
import {
  GEOMETRY_ICON,
  describeGeometry,
  geometryIcon,
  geometryTitle,
  isGeometryColumn,
  isWktValue,
  parseWkt,
} from "./geometry";
import type { FileColumn } from "../types/node.types";

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
  '{"type":"Point","coordinates":[4.9,52.3]}',
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

describe("describeGeometry", () => {
  it("shows the coordinate for a single point", () => {
    expect(describeGeometry("POINT (4.9041 52.3676)")).toBe("Point (4.9041, 52.3676)");
  });

  it("shows shape and vertex count for everything else", () => {
    expect(describeGeometry("POLYGON ((0 0, 1 0, 1 1, 0 0))")).toBe("Polygon · 4 pts");
    expect(describeGeometry("MULTIPOINT (1 2, 3 4)")).toBe("MultiPoint · 2 pts");
  });

  it("marks empty geometry and carries the dimension tag", () => {
    expect(describeGeometry("POLYGON EMPTY")).toBe("Polygon (empty)");
    expect(describeGeometry("POINT Z (1 2 3)")).toBe("Point Z (1, 2)");
  });

  it("returns null for a non-geometry value so the caller can fall back", () => {
    expect(describeGeometry("Amsterdam")).toBeNull();
    expect(describeGeometry(null)).toBeNull();
  });
});

describe("isGeometryColumn", () => {
  it("trusts only the semantic type core derived from a declared dtype", () => {
    expect(isGeometryColumn({ semantic_type: "geometry" })).toBe(true);
    expect(isGeometryColumn({ semantic_type: null })).toBe(false);
    expect(isGeometryColumn({})).toBe(false);
  });

  it("never infers geometry from a column's name, dtype or values", () => {
    const column = {
      name: "geometry",
      data_type: "String",
      data_type_group: "String",
    } as FileColumn;
    expect(isGeometryColumn(column)).toBe(false);
  });
});

describe("geometryTitle", () => {
  it("names the storage dtype so the label never hides what Select can cast", () => {
    expect(geometryTitle("Binary")).toBe("Geometry (GeoArrow), stored as Binary");
    expect(geometryTitle(undefined)).toBe("Geometry (GeoArrow)");
  });
});

describe("geometryIcon", () => {
  it("picks a shape icon from WKT and the generic mark for anything else", () => {
    expect(geometryIcon("POINT (1 2)")).toBe("place");
    expect(geometryIcon("MULTIPOINT (1 2, 3 4)")).toBe("place");
    expect(geometryIcon("LINESTRING (0 0, 1 1)")).toBe("polyline");
    expect(geometryIcon("POLYGON ((0 0, 1 0, 1 1, 0 0))")).toBe(GEOMETRY_ICON);
    expect(geometryIcon("GEOMETRYCOLLECTION (POINT (1 2))")).toBe("layers");
    expect(geometryIcon("0x0101000000EE5A423EE8991340F1F44A5986304A40")).toBe(GEOMETRY_ICON);
    expect(geometryIcon(null)).toBe(GEOMETRY_ICON);
  });
});
